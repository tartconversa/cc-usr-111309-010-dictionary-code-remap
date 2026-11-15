"""分区字典编码的全局统一、可逆重编码与版本化发布。

设计约束（与 ``fixtures/dictionary.json`` 中的协议一致）：

* 每个分区有自己的 ``local_dictionary``（精确 UTF-8 字符串的有序元组）与
  ``codes`` 整数列。code 是 *本地* 数字，不是业务身份；语义值只能由
  ``local_dictionary[code]`` 得到，不能把编码列直接拼接。
* NULL 由独立 validity 位图表示。``code == -1`` 不代表 NULL；无效位置的 code
  原样保留（供审计/回写），但不参与语义值。空字符串 ``""`` 是合法字典值。
* 全局字典按 UTF-8 字节序排序去重；各分区 local_code -> global_code 的映射
  显式存储，绝不按本地数字直接合并。
* ``nfc`` 归一化是另一个 *编码协议版本*，必须换 dictionary_id；默认 ``none``。
  启用它是显式的新构建，不会修改既有字典。
* 追加分区会改变全局排序，从而让既有 global_code 整体平移。因此新内容必须
  发布为新版本（新 dictionary_id + 摘要），不能原地覆盖旧字典。旧列永远用
  它绑定的旧版本字典解释。

公开 API：

``build_global_dictionary``            由分区构建公共字典与每分区转换表。
``build_global_dictionary_streaming``  两遍流式构建（字典外排序，不整体载入）。
``external_sort_entries``              有界内存的外部归并排序。
``remap_partition``                    把本地 code 列重编码为 global code 列。
``stream_remap`` / ``stream_write_remap``  大列流式改码，O(1) 内存。
``verify_semantics``                   反向语义核对（解码回字符串）。
``Publisher`` / ``PublishedVersion``   原子发布新字典及全部目标列映射。
``stable_codes``                       追加式稳定编码（可选协议，顺序语义不同）。
"""

from __future__ import annotations

import csv
import hashlib
import heapq
import json
import os
import shutil
import tempfile
import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence, Sized
from dataclasses import dataclass, replace
from itertools import zip_longest
from pathlib import Path

PROTOCOL_VERSION = "exact_utf8_bytes_sorted/v1"
NFC_PROTOCOL_VERSION = "nfc_utf8_bytes_sorted/v1"
STABLE_PROTOCOL_VERSION = "append_order_stable/v1"
NULL_ENCODING = "separate_validity"
SUPPORTED_NORMALIZATIONS = ("none", "nfc")

# -1 是常见的“缺值”哨兵；本协议只把它当普通整数，valid 位置上会因越界报错。
INVALID_CODE_SENTINEL = -1

DEFAULT_MAX_BUFFER_BYTES = 512


class DictionaryError(ValueError):
    """输入字典/编码列违反契约。"""


class EncodingRangeError(DictionaryError):
    """valid 位置的 code 超出本地字典范围。"""


class DuplicateDictionaryEntryError(DictionaryError):
    """本地字典含重复项。"""


class ValidityError(DictionaryError):
    """validity 位图与 code 列不同长或含非 0/1 值。"""


class NormalizationMismatchError(DictionaryError):
    """试图用不同归一化协议读取既有字典。"""


class DictionaryVersionError(DictionaryError):
    """dictionary_id / 摘要不匹配，或试图原地替换既有版本。"""


class ReleaseIncompleteError(DictionaryError):
    """版本目录内的发布未完成。"""


class StreamBufferError(DictionaryError):
    """流式/缓冲参数非法。"""


# ---------------------------------------------------------------------------
# 基本校验、归一化与排序


def check_normalization(unicode_normalization: str) -> str:
    if unicode_normalization not in SUPPORTED_NORMALIZATIONS:
        raise NormalizationMismatchError(
            f"不支持的归一化协议: {unicode_normalization!r}; "
            f"可用: {', '.join(SUPPORTED_NORMALIZATIONS)}"
        )
    return unicode_normalization


def protocol_for(unicode_normalization: str) -> str:
    check_normalization(unicode_normalization)
    return NFC_PROTOCOL_VERSION if unicode_normalization == "nfc" else PROTOCOL_VERSION


def normalize_entry(value: str, unicode_normalization: str = "none") -> str:
    """按协议归一化一个字典项；不 trim、不折叠大小写，空串保持空串。"""
    if not isinstance(value, str):
        raise DictionaryError(f"字典项必须是字符串，得到 {type(value).__name__}")
    if unicode_normalization == "nfc":
        return unicodedata.normalize("NFC", value)
    return value


def byte_sort_key(value: str) -> bytes:
    """全局字典的排序键：精确 UTF-8 字节序。

    使用显式字节序而不是本地区域设置/locale 排序；多字节字符按编码字节比较。
    """
    return value.encode("utf-8")


def dictionary_digest(entries: Iterable[str], *, protocol: str) -> str:
    """摘要绑定协议版本 + 有序项 + UTF-8 字节长度前缀。"""
    digest = hashlib.sha256()
    digest.update(protocol.encode("ascii"))
    digest.update(b"\x00")
    for entry in entries:
        raw = entry.encode("utf-8")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def dictionary_id_for(entries: Sequence[str], *, protocol: str) -> str:
    short = dictionary_digest(entries, protocol=protocol)[:16]
    return f"{protocol.split('/')[0]}-{short}"


def _find_global_index(entries: Sequence[str], value: str) -> int:
    """在按 UTF-8 字节序排序的 entries 中二分查找；缺失抛 KeyError。"""
    needle = value.encode("utf-8")
    low, high = 0, len(entries)
    while low < high:
        middle = (low + high) // 2
        probe = entries[middle].encode("utf-8")
        if probe < needle:
            low = middle + 1
        else:
            high = middle
    if low >= len(entries) or entries[low].encode("utf-8") != needle:
        raise KeyError(value)
    return low


def _reject_non_string_dictionary(local_dictionary: Sequence[str]) -> None:
    if isinstance(local_dictionary, (str, bytes, bytearray)):
        raise DictionaryError("本地字典必须是字符串序列，不是单个字符串/字节串")
    for index, item in enumerate(local_dictionary):
        if isinstance(item, (bytes, bytearray)):
            raise DictionaryError(
                f"字典项 #{index} 是字节串；必须预先解码为精确 UTF-8 字符串"
            )
        if not isinstance(item, str):
            raise DictionaryError(f"字典项 #{index} 必须是字符串，得到 {type(item).__name__}")


def _require_int(value: object, *, row: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DictionaryError(f"行 {row}: {field} 必须是整数，得到 {value!r}")
    return value


def _require_bit(value: object, *, row: int) -> int:
    if isinstance(value, bool):
        raise ValidityError(f"行 {row}: validity 必须是 0 或 1，得到布尔值")
    if not isinstance(value, int) or value not in (0, 1):
        raise ValidityError(f"行 {row}: validity 必须是 0 或 1，得到 {value!r}")
    return value


def validate_partition(
    local_dictionary: Sequence[str],
    codes: Sequence[int],
    validity: Sequence[int],
    *,
    unicode_normalization: str = "none",
) -> tuple[tuple[str, ...], tuple[int, ...], tuple[int, ...]]:
    """校验分区并返回 (归一化字典, 原始码元组, validity 元组)。

    * 字典项必须是字符串，精确字符串不允许重复（重复项使本地码有歧义）。
    * validity 与 codes 等长且只含 0/1。
    * 所有 valid 位置的 code 必须在 ``[0, len(dictionary))``；无效位置原 code
      可任意（包括 -1），保留但不参与语义。
    * NFC 协议下，不同本地字典项归一化后可能指向同一公共值；这是协议声明的
      合并行为，转换表仍逐项显式记录。
    """
    check_normalization(unicode_normalization)
    _reject_non_string_dictionary(local_dictionary)

    original = tuple(local_dictionary)
    if len(set(original)) != len(original):
        duplicate = next(item for item in original if original.count(item) > 1)
        raise DuplicateDictionaryEntryError(f"本地字典含重复项: {duplicate!r}")
    normalized = tuple(normalize_entry(item, unicode_normalization) for item in original)
    if unicode_normalization != "none" and len(set(normalized)) != len(normalized):
        clash = next(item for item in normalized if normalized.count(item) > 1)
        raise DuplicateDictionaryEntryError(
            f"{unicode_normalization} 协议下本地字典归一化后重复: {clash!r}"
        )

    codes = tuple(codes)
    validity = tuple(validity)
    if len(codes) != len(validity):
        raise ValidityError(
            f"codes 长度 {len(codes)} 与 validity 长度 {len(validity)} 不一致"
        )

    upper = len(normalized)
    for row, (raw_code, bit) in enumerate(zip(codes, validity)):
        code = _require_int(raw_code, row=row, field="code")
        bit_value = _require_bit(bit, row=row)
        if bit_value == 1 and not 0 <= code < upper:
            raise EncodingRangeError(
                f"行 {row}: valid code {code} 超出本地字典范围 [0, {upper})"
            )
    return normalized, codes, validity


# ---------------------------------------------------------------------------
# 字典外排序（有界内存归并）


class _Missing:
    def __repr__(self) -> str:  # pragma: no cover - 仅调试
        return "<missing>"


_MISSING = _Missing()


# 外部排序 run 与磁盘字典共用的记录格式：每行一个 JSON 字符串。


def _write_run(directory: Path, index: int, entries: Iterable[str]) -> Path:
    path = directory / f"run-{index:06d}.jsonl"
    with path.open("w", encoding="utf-8") as stream:
        for entry in entries:
            # 每项一行 JSON 字符串；换行/控制字符被转义，可无歧义回读。
            stream.write(json.dumps(entry, ensure_ascii=False))
            stream.write("\n")
    return path


def _read_run(path: Path) -> Iterator[str]:
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def external_sort_entries(
    values: Iterable[str],
    *,
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
    temp_dir: Path | str | None = None,
) -> tuple[str, ...]:
    """按 UTF-8 字节序排序去重，任意时刻内存不超过 ``max_buffer_bytes`` 的块。

    块内排序去重后顺序写入临时 run 文件，最后 k 路归并去重。返回值本身是完整
    有序字典；若字典本身大于内存，请改用 :func:`external_sort_to_file` 与
    :class:`SortedDictionaryFile`，它们不会把整个字典载入内存。
    """
    return tuple(
        _external_sort(
            values, max_buffer_bytes=max_buffer_bytes, temp_dir=temp_dir
        )
    )


def _external_sort(
    values: Iterable[str],
    *,
    max_buffer_bytes: int,
    temp_dir: Path | str | None,
) -> Iterator[str]:
    if max_buffer_bytes <= 0:
        raise StreamBufferError("max_buffer_bytes 必须为正")
    root = Path(tempfile.mkdtemp(prefix="dict-sort-", dir=temp_dir))
    runs: list[Path] = []
    chunk: list[str] = []
    chunk_bytes = 0

    def flush() -> None:
        nonlocal chunk, chunk_bytes
        if not chunk:
            return
        unique = sorted(set(chunk), key=byte_sort_key)
        runs.append(_write_run(root, len(runs), unique))
        chunk = []
        chunk_bytes = 0

    try:
        for value in values:
            if not isinstance(value, str):
                raise DictionaryError(f"字典项必须是字符串，得到 {type(value).__name__}")
            chunk.append(value)
            chunk_bytes += len(value.encode("utf-8")) + 16
            if chunk_bytes >= max_buffer_bytes:
                flush()
        flush()
        previous: object = _MISSING
        for entry in heapq.merge(
            *(_read_run(path) for path in runs), key=byte_sort_key
        ):
            if entry != previous:
                previous = entry
                yield entry
    finally:
        shutil.rmtree(root, ignore_errors=True)


class SortedDictionaryFile:
    """磁盘上的有序公共字典，允许字典大于内存。

    数据文件每行一个 JSON 字符串（UTF-8 字节升序、已去重）；稀疏索引每
    ``index_stride`` 项记录 ``(UTF-8 键, 字节偏移)``。随机查找先二分索引，再在
    一个块内顺序扫描。索引与块大小都由构造参数限定。
    """

    def __init__(
        self,
        data_path: Path | str,
        index_path: Path | str,
        index: Sequence[tuple[str, int]],
        *,
        index_stride: int,
        total_rows: int,
    ):
        if index_stride <= 0:
            raise StreamBufferError("index_stride 必须为正")
        self.data_path = Path(data_path)
        self.index_path = Path(index_path)
        self.index_stride = index_stride
        self.total_rows = total_rows
        self._index = tuple(index)
        self._entries: tuple[str, ...] | None = None

    def __len__(self) -> int:
        return self.total_rows

    def iter_entries(self) -> Iterator[str]:
        with self.data_path.open(encoding="utf-8") as stream:
            for line in stream:
                yield json.loads(line)

    def index_of(self, value: str) -> int:
        """返回 ``value`` 的 global_code；缺失抛 KeyError。"""
        if not isinstance(value, str):
            raise DictionaryError(f"字典项必须是字符串，得到 {type(value).__name__}")
        needle = value.encode("utf-8")
        if not self._index:
            raise KeyError(value)
        # 找最后一个索引键 <= needle 的检查点，并从该处线性推进。
        low, high = 0, len(self._index)
        while low < high:
            middle = (low + high) // 2
            if self._index[middle][0].encode("utf-8") <= needle:
                low = middle + 1
            else:
                high = middle
        checkpoint = max(low - 1, 0)
        position = checkpoint * self.index_stride
        with self.data_path.open("rb") as stream:
            stream.seek(self._index[checkpoint][1])
            for line in stream:
                entry = json.loads(line.decode("utf-8"))
                raw = entry.encode("utf-8")
                if raw == needle:
                    return position
                if raw > needle:
                    break
                position += 1
        raise KeyError(value)

    def materialize(self) -> tuple[str, ...]:
        """把完整字典载入内存（仅当确认放得下时使用）。"""
        if self._entries is None:
            self._entries = tuple(self.iter_entries())
        return self._entries

    def digest(self, *, protocol: str) -> str:
        """按与 :func:`dictionary_digest` 相同的帧格式流式计算摘要。"""
        digest = hashlib.sha256()
        digest.update(protocol.encode("ascii"))
        digest.update(b"\x00")
        for entry in self.iter_entries():
            raw = entry.encode("utf-8")
            digest.update(len(raw).to_bytes(8, "big"))
            digest.update(raw)
        return digest.hexdigest()

    def value_of(self, global_code: int) -> str:
        code = _require_int(global_code, row=-1, field="global_code")
        if not 0 <= code < self.total_rows:
            raise EncodingRangeError(
                f"global_code {code} 超出公共字典范围 [0, {self.total_rows})"
            )
        checkpoint = code // self.index_stride
        position = checkpoint * self.index_stride
        with self.data_path.open("rb") as stream:
            stream.seek(self._index[checkpoint][1])
            for line in stream:
                if position == code:
                    return json.loads(line.decode("utf-8"))
                position += 1
        raise DictionaryVersionError(f"磁盘字典缺少 global_code {code}")


def external_sort_to_file(
    values: Iterable[str],
    data_path: Path | str,
    *,
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
    index_stride: int = 1024,
    temp_dir: Path | str | None = None,
) -> SortedDictionaryFile:
    """流式排序去重并直接落盘，同时写出稀疏索引。

    全程只保留一个块；``index_stride`` 决定索引粒度（越小查找越快、索引越大）。
    """
    if index_stride <= 0:
        raise StreamBufferError("index_stride 必须为正")
    target = Path(data_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    index_path = target.with_suffix(target.suffix + ".idx")
    index: list[tuple[str, int]] = []
    offset = 0
    position = 0
    with target.open("wb") as stream:
        for entry in _external_sort(
            values, max_buffer_bytes=max_buffer_bytes, temp_dir=temp_dir
        ):
            if position % index_stride == 0:
                index.append((entry, offset))
            line = (json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8")
            stream.write(line)
            offset += len(line)
            position += 1
    with index_path.open("w", encoding="utf-8") as stream:
        for entry, entry_offset in index:
            stream.write(json.dumps([entry, entry_offset], ensure_ascii=False) + "\n")
    return SortedDictionaryFile(
        target,
        index_path,
        index,
        index_stride=index_stride,
        total_rows=position,
    )


@dataclass
class DiskGlobalDictionary:
    """字典大于内存时的公共字典：项在磁盘上，映射与小表在内存。

    与 :class:`GlobalDictionary` 的查找接口一致，所以 ``remap_partition`` /
    ``verify_semantics`` 可直接接受它。``entries`` 不提供：需要完整列表时显式
    调用 ``materialize_entries()``，避免误把整本字典读进内存。
    """

    file: SortedDictionaryFile
    dictionary_id: str
    dictionary_digest: str
    protocol: str
    unicode_normalization: str
    mappings: tuple[PartitionMapping, ...] = ()
    supersedes: str | None = None

    def index_of(self, value: str) -> int:
        return self.file.index_of(normalize_entry(value, self.unicode_normalization))

    def value_of(self, global_code: int) -> str:
        return self.file.value_of(global_code)

    def mapping_for(self, partition_key: str) -> PartitionMapping:
        for mapping in self.mappings:
            if mapping.partition_key == partition_key:
                return mapping
        raise KeyError(partition_key)

    def materialize_entries(self) -> tuple[str, ...]:
        return self.file.materialize()

    def to_json(self) -> dict:
        return {
            "protocol": self.protocol,
            "unicode_normalization": self.unicode_normalization,
            "null_encoding": NULL_ENCODING,
            "dictionary_id": self.dictionary_id,
            "dictionary_digest": self.dictionary_digest,
            "entries_file": "entries.jsonl",
            "entries_index": "entries.jsonl.idx",
            "entry_count": self.file.total_rows,
            "index_stride": self.file.index_stride,
            "supersedes": self.supersedes,
            "mappings": [mapping.to_json() for mapping in self.mappings],
        }

    @classmethod
    def from_json(cls, data: Mapping, directory: Path | str) -> "DiskGlobalDictionary":
        root = Path(directory)
        data_path = root / "entries" / data["entries_file"]
        index_path = root / "entries" / data["entries_index"]
        index = []
        with index_path.open(encoding="utf-8") as stream:
            for line in stream:
                entry, offset = json.loads(line)
                index.append((entry, offset))
        dictionary = cls(
            file=SortedDictionaryFile(
                data_path,
                index_path,
                index,
                index_stride=data["index_stride"],
                total_rows=data["entry_count"],
            ),
            dictionary_id=data["dictionary_id"],
            dictionary_digest=data["dictionary_digest"],
            protocol=data["protocol"],
            unicode_normalization=data.get("unicode_normalization", "none"),
            mappings=tuple(
                PartitionMapping.from_json(item) for item in data.get("mappings", ())
            ),
            supersedes=data.get("supersedes"),
        )
        probe = dictionary.file.digest(protocol=dictionary.protocol)
        if probe != dictionary.dictionary_digest:
            raise DictionaryVersionError("磁盘字典文件摘要与声明不一致")
        return dictionary


def build_disk_dictionary(
    partition_source: Callable[[], Iterable[Partition | Mapping]],
    directory: Path | str,
    *,
    unicode_normalization: str = "none",
    supersedes: str | None = None,
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
    index_stride: int = 1024,
    temp_dir: Path | str | None = None,
) -> DiskGlobalDictionary:
    """两遍构建 + 磁盘有序字典；全量字典不驻留内存。

    第一遍外部排序直接落盘；第二遍只保留每分区的本地字典与 validity 汇总，
    用磁盘字典的随机查找生成 local_to_global（每个本地项一次查找，不载入全表）。
    """
    protocol = protocol_for(unicode_normalization)
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)

    def entry_stream() -> Iterator[str]:
        seen: set[str] = set()
        for item in partition_source():
            partition = _as_partition(item)
            if partition.key in seen:
                raise DictionaryError(f"分区 key 重复: {partition.key!r}")
            seen.add(partition.key)
            local, _, _ = validate_partition(
                partition.local_dictionary,
                partition.codes,
                partition.validity,
                unicode_normalization=unicode_normalization,
            )
            yield from local

    sorted_file = external_sort_to_file(
        entry_stream(),
        root / "entries.jsonl",
        max_buffer_bytes=max_buffer_bytes,
        index_stride=index_stride,
        temp_dir=temp_dir,
    )
    digest = sorted_file.digest(protocol=protocol)
    version = f"{protocol.split('/')[0]}-{digest[:16]}"

    prepared: list[tuple[str, tuple[str, ...], int]] = []
    seen_keys: set[str] = set()
    for item in partition_source():
        partition = _as_partition(item)
        if partition.key in seen_keys:
            raise DictionaryError(f"分区 key 重复: {partition.key!r}")
        seen_keys.add(partition.key)
        local, _, validity = validate_partition(
            partition.local_dictionary,
            partition.codes,
            partition.validity,
            unicode_normalization=unicode_normalization,
        )
        prepared.append((partition.key, local, sum(1 for bit in validity if bit == 1)))
    if not prepared:
        raise DictionaryError("至少需要一个分区")

    mappings = tuple(
        PartitionMapping(
            partition_key=key,
            local_dictionary=local,
            local_to_global=tuple(sorted_file.index_of(entry) for entry in local),
            dictionary_id=version,
            dictionary_digest=digest,
            protocol=protocol,
            unicode_normalization=unicode_normalization,
            valid_value_count=valid_count,
        )
        for key, local, valid_count in prepared
    )
    return DiskGlobalDictionary(
        file=sorted_file,
        dictionary_id=version,
        dictionary_digest=digest,
        protocol=protocol,
        unicode_normalization=unicode_normalization,
        mappings=mappings,
        supersedes=supersedes,
    )


# ---------------------------------------------------------------------------
# 公共字典与显式转换表


@dataclass(frozen=True)
class PartitionMapping:
    """一个分区到公共字典的显式映射，绑定 dictionary_id + 摘要。"""

    partition_key: str
    local_dictionary: tuple[str, ...]
    local_to_global: tuple[int, ...]
    dictionary_id: str
    dictionary_digest: str
    protocol: str
    unicode_normalization: str
    valid_value_count: int = 0

    def to_json(self) -> dict:
        return {
            "partition_key": self.partition_key,
            "local_dictionary": list(self.local_dictionary),
            "local_to_global": list(self.local_to_global),
            "dictionary_id": self.dictionary_id,
            "dictionary_digest": self.dictionary_digest,
            "protocol": self.protocol,
            "unicode_normalization": self.unicode_normalization,
            "valid_value_count": self.valid_value_count,
        }

    @classmethod
    def from_json(cls, data: Mapping) -> "PartitionMapping":
        return cls(
            partition_key=data["partition_key"],
            local_dictionary=tuple(data["local_dictionary"]),
            local_to_global=tuple(data["local_to_global"]),
            dictionary_id=data["dictionary_id"],
            dictionary_digest=data["dictionary_digest"],
            protocol=data["protocol"],
            unicode_normalization=data.get("unicode_normalization", "none"),
            valid_value_count=data.get("valid_value_count", 0),
        )


@dataclass(frozen=True)
class GlobalDictionary:
    """一个版本化的公共字典。"""

    entries: tuple[str, ...]
    dictionary_id: str
    dictionary_digest: str
    protocol: str
    unicode_normalization: str
    mappings: tuple[PartitionMapping, ...] = ()
    supersedes: str | None = None

    def index_of(self, value: str) -> int:
        """按 UTF-8 字节序二分查找语义值对应的 global_code。"""
        needle = normalize_entry(value, self.unicode_normalization)
        return _find_global_index(self.entries, needle)

    def value_of(self, global_code: int) -> str:
        code = _require_int(global_code, row=-1, field="global_code")
        if not 0 <= code < len(self.entries):
            raise EncodingRangeError(
                f"global_code {code} 超出公共字典范围 [0, {len(self.entries)})"
            )
        return self.entries[code]

    def mapping_for(self, partition_key: str) -> PartitionMapping:
        for mapping in self.mappings:
            if mapping.partition_key == partition_key:
                return mapping
        raise KeyError(partition_key)

    def to_json(self) -> dict:
        return {
            "protocol": self.protocol,
            "unicode_normalization": self.unicode_normalization,
            "null_encoding": NULL_ENCODING,
            "dictionary_id": self.dictionary_id,
            "dictionary_digest": self.dictionary_digest,
            "entries": list(self.entries),
            "supersedes": self.supersedes,
            "mappings": [mapping.to_json() for mapping in self.mappings],
        }

    @classmethod
    def from_json(cls, data: Mapping) -> "GlobalDictionary":
        return cls(
            entries=tuple(data["entries"]),
            dictionary_id=data["dictionary_id"],
            dictionary_digest=data["dictionary_digest"],
            protocol=data["protocol"],
            unicode_normalization=data.get("unicode_normalization", "none"),
            mappings=tuple(
                PartitionMapping.from_json(item) for item in data.get("mappings", ())
            ),
            supersedes=data.get("supersedes"),
        )


@dataclass
class Partition:
    """内存中的分区输入。"""

    key: str
    local_dictionary: Sequence[str]
    codes: Sequence[int]
    validity: Sequence[int]

    @classmethod
    def from_json(cls, data: Mapping) -> "Partition":
        if "local_dictionary" in data:
            local_dictionary = data["local_dictionary"]
        elif "dictionary" in data:
            local_dictionary = data["dictionary"]
        else:
            raise DictionaryError("分区缺少 dictionary/local_dictionary")
        return cls(
            key=data["key"],
            local_dictionary=local_dictionary,
            codes=data["codes"],
            validity=data["validity"],
        )


def _as_partition(item: Partition | Mapping) -> Partition:
    if isinstance(item, Partition):
        return item
    if not isinstance(item, Mapping):
        raise DictionaryError(f"分区必须是 Partition 或映射，得到 {type(item).__name__}")
    return Partition.from_json(item)


def _mapping_for(
    key: str,
    local_dictionary: tuple[str, ...],
    entries: tuple[str, ...],
    *,
    version: str,
    digest: str,
    protocol: str,
    unicode_normalization: str,
    valid_value_count: int,
) -> PartitionMapping:
    # 逐个按字符串查表；NFC 合并后多个本地码可指向同一 global_code。
    return PartitionMapping(
        partition_key=key,
        local_dictionary=local_dictionary,
        local_to_global=tuple(
            _find_global_index(entries, entry) for entry in local_dictionary
        ),
        dictionary_id=version,
        dictionary_digest=digest,
        protocol=protocol,
        unicode_normalization=unicode_normalization,
        valid_value_count=valid_value_count,
    )


def build_global_dictionary(
    partitions: Iterable[Partition | Mapping],
    *,
    unicode_normalization: str = "none",
    protocol: str | None = None,
    supersedes: str | None = None,
    max_buffer_bytes: int | None = None,
    temp_dir: Path | str | None = None,
) -> GlobalDictionary:
    """构建公共字典与逐分区映射。

    输入顺序不影响结果；公共字典严格按 UTF-8 字节序排序去重。``protocol``
    默认为归一化选项对应的协议；显式传入不一致的 protocol 会被拒绝，避免把
    NFC 版本冒充成既有 ``none`` 版本。

    ``max_buffer_bytes`` 非空时走外部归并排序（有界内存），适合超大并集。
    """
    expected_protocol = protocol_for(unicode_normalization)
    if protocol is not None and protocol != expected_protocol:
        raise NormalizationMismatchError(
            f"protocol {protocol!r} 与归一化选项 {unicode_normalization!r} 不匹配；"
            f"应使用 {expected_protocol!r}"
        )
    protocol = expected_protocol

    prepared: list[tuple[str, tuple[str, ...], tuple[int, ...], int]] = []
    seen_keys: set[str] = set()
    for item in partitions:
        partition = _as_partition(item)
        if partition.key in seen_keys:
            raise DictionaryError(f"分区 key 重复: {partition.key!r}")
        seen_keys.add(partition.key)
        local, codes, validity = validate_partition(
            partition.local_dictionary,
            partition.codes,
            partition.validity,
            unicode_normalization=unicode_normalization,
        )
        prepared.append((partition.key, local, codes, sum(1 for bit in validity if bit == 1)))
    if not prepared:
        raise DictionaryError("至少需要一个分区")

    if max_buffer_bytes is None:
        union: set[str] = set()
        for _, local, _, _ in prepared:
            union.update(local)
        entries = tuple(sorted(union, key=byte_sort_key))
    else:
        entries = external_sort_entries(
            (entry for _, local, _, _ in prepared for entry in local),
            max_buffer_bytes=max_buffer_bytes,
            temp_dir=temp_dir,
        )
    return _finalize_dictionary(
        prepared,
        entries,
        protocol=protocol,
        unicode_normalization=unicode_normalization,
        supersedes=supersedes,
    )


def build_global_dictionary_streaming(
    partition_source: Callable[[], Iterable[Partition | Mapping]],
    *,
    unicode_normalization: str = "none",
    supersedes: str | None = None,
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
    temp_dir: Path | str | None = None,
) -> GlobalDictionary:
    """两遍流式构建：第一遍外部排序收集字典，第二遍只读本地字典生成映射。

    ``partition_source`` 必须能在同一进程内被调用两次并产生相同内容；分区本体
    （codes/validity 大列）不会在第一遍被保留。分区 key 去重与 code 范围校验
    在两遍中都会执行。
    """
    protocol = protocol_for(unicode_normalization)

    def entry_stream() -> Iterator[str]:
        seen_keys: set[str] = set()
        for item in partition_source():
            partition = _as_partition(item)
            if partition.key in seen_keys:
                raise DictionaryError(f"分区 key 重复: {partition.key!r}")
            seen_keys.add(partition.key)
            local, _, _ = validate_partition(
                partition.local_dictionary,
                partition.codes,
                partition.validity,
                unicode_normalization=unicode_normalization,
            )
            yield from local

    entries = external_sort_entries(
        entry_stream(), max_buffer_bytes=max_buffer_bytes, temp_dir=temp_dir
    )
    prepared = []
    for item in partition_source():
        partition = _as_partition(item)
        local, _, validity = validate_partition(
            partition.local_dictionary,
            partition.codes,
            partition.validity,
            unicode_normalization=unicode_normalization,
        )
        prepared.append(
            (partition.key, local, (), sum(1 for bit in validity if bit == 1))
        )
    if not prepared:
        raise DictionaryError("至少需要一个分区")
    return _finalize_dictionary(
        prepared,
        entries,
        protocol=protocol,
        unicode_normalization=unicode_normalization,
        supersedes=supersedes,
    )


def _finalize_dictionary(
    prepared: Sequence[tuple[str, tuple[str, ...], tuple[int, ...], int]],
    entries: tuple[str, ...],
    *,
    protocol: str,
    unicode_normalization: str,
    supersedes: str | None,
) -> GlobalDictionary:
    digest = dictionary_digest(entries, protocol=protocol)
    version = dictionary_id_for(entries, protocol=protocol)
    mappings = tuple(
        _mapping_for(
            key,
            local,
            entries,
            version=version,
            digest=digest,
            protocol=protocol,
            unicode_normalization=unicode_normalization,
            valid_value_count=valid_count,
        )
        for key, local, _, valid_count in prepared
    )
    dictionary = GlobalDictionary(
        entries=entries,
        dictionary_id=version,
        dictionary_digest=digest,
        protocol=protocol,
        unicode_normalization=unicode_normalization,
        mappings=mappings,
        supersedes=supersedes,
    )
    validate_global_dictionary(dictionary)
    return dictionary


def validate_global_dictionary(dictionary: GlobalDictionary) -> None:
    """校验公共字典自洽：排序、去重、摘要、映射绑定与转换表指向。"""
    if not dictionary.entries:
        raise DictionaryError("公共字典为空")
    if tuple(sorted(dictionary.entries, key=byte_sort_key)) != dictionary.entries:
        raise DictionaryError("公共字典未按 UTF-8 字节序排序")
    if len(set(dictionary.entries)) != len(dictionary.entries):
        raise DuplicateDictionaryEntryError("公共字典含重复项")
    if dictionary.protocol != protocol_for(dictionary.unicode_normalization):
        raise NormalizationMismatchError("字典协议与归一化选项不匹配")
    if dictionary_digest(dictionary.entries, protocol=dictionary.protocol) != dictionary.dictionary_digest:
        raise DictionaryVersionError("dictionary_digest 与 entries 不匹配")
    if dictionary.dictionary_id != dictionary_id_for(
        dictionary.entries, protocol=dictionary.protocol
    ):
        raise DictionaryVersionError("dictionary_id 与 entries 不匹配")
    _validate_mappings(dictionary)
    known = set(dictionary.entries)
    for mapping in dictionary.mappings:
        if set(mapping.local_dictionary) - known:
            raise DictionaryVersionError(
                f"分区 {mapping.partition_key}: 本地字典含公共字典外值"
            )
        for code in mapping.local_to_global:
            if not 0 <= code < len(dictionary.entries):
                raise DictionaryVersionError(
                    f"分区 {mapping.partition_key}: 转换表指向越界 global_code {code}"
                )
        resolved = tuple(dictionary.entries[code] for code in mapping.local_to_global)
        if resolved != mapping.local_dictionary:
            raise DictionaryVersionError(
                f"分区 {mapping.partition_key}: 转换表未正确指向公共字典"
            )


def partitions_from_document(
    document: Mapping,
    *,
    include_append: bool = False,
) -> list[Partition]:
    """读取 ``{"partitions": [...], "append_partition": {...}}`` 形态的输入。"""
    if not isinstance(document, Mapping):
        raise DictionaryError("分区文档必须是 JSON 对象")
    raw = list(document.get("partitions", ()))
    if include_append:
        appended = document.get("append_partition")
        if appended is not None:
            raw.append(appended)
    if not raw:
        raise DictionaryError("输入中没有分区")
    return [_as_partition(item) for item in raw]


def build_from_partitions_document(
    document: Mapping,
    *,
    include_append: bool = False,
    unicode_normalization: str = "none",
    supersedes: str | None = None,
) -> GlobalDictionary:
    return build_global_dictionary(
        partitions_from_document(document, include_append=include_append),
        unicode_normalization=unicode_normalization,
        supersedes=supersedes,
    )


# ---------------------------------------------------------------------------
# 列重编码


@dataclass
class RemapResult:
    """单列重编码结果，绑定其版本字典。"""

    partition_key: str
    dictionary_id: str
    dictionary_digest: str
    global_codes: list[int | None]
    preserved_invalid_codes: list[int | None]
    logical_values: list[str | None]
    validity: list[int]
    row_count: int

    @property
    def logical_multiset(self) -> dict[str, int]:
        """只统计 valid 位置的逻辑值（多重集），用于重编码前后核对。"""
        counts: dict[str, int] = {}
        for value in self.logical_values:
            if value is not None:
                counts[value] = counts.get(value, 0) + 1
        return counts


@dataclass
class RemapBatch:
    """多个分区的重编码结果，共享同一版本字典。"""

    dictionary: GlobalDictionary | DiskGlobalDictionary
    results: tuple[RemapResult, ...]

    def for_partition(self, partition_key: str) -> RemapResult:
        for result in self.results:
            if result.partition_key == partition_key:
                return result
        raise KeyError(partition_key)


def _check_mapping_binding(dictionary: GlobalDictionary | DiskGlobalDictionary, mapping: PartitionMapping) -> None:
    if mapping.dictionary_id != dictionary.dictionary_id:
        raise DictionaryVersionError(
            f"分区 {mapping.partition_key} 映射绑定了其他 dictionary_id；"
            "旧列必须用旧字典解释"
        )
    if mapping.dictionary_digest != dictionary.dictionary_digest:
        raise DictionaryVersionError(f"分区 {mapping.partition_key} 映射摘要与字典不一致")


def remap_partition(
    dictionary: GlobalDictionary | DiskGlobalDictionary,
    partition_key: str,
    codes: Sequence[int],
    validity: Sequence[int],
    *,
    keep_invalid_code: bool = True,
) -> RemapResult:
    """把某分区的本地列重编码为公共码。

    行序、长度、validity 位置完全保持。无效位置输出 ``None``（语义 NULL）；
    ``keep_invalid_code=True`` 时额外保留原 code 供审计。转换使用该分区绑定
    的 local_to_global 表，不假设本地码与全局码数字相同。
    """
    try:
        mapping = dictionary.mapping_for(partition_key)
    except KeyError:
        raise DictionaryError(
            f"分区 {partition_key!r} 不在字典 {dictionary.dictionary_id} 的映射中"
        ) from None
    _check_mapping_binding(dictionary, mapping)

    codes = tuple(codes)
    validity = tuple(validity)
    if len(codes) != len(validity):
        raise ValidityError(
            f"分区 {partition_key}: codes 长度 {len(codes)} 与 validity 长度 {len(validity)} 不一致"
        )
    local_to_global = mapping.local_to_global
    upper = len(local_to_global)

    global_codes: list[int | None] = []
    preserved: list[int | None] = []
    logical: list[str | None] = []
    for row, (raw_code, bit) in enumerate(zip(codes, validity)):
        code = _require_int(raw_code, row=row, field="code")
        bit_value = _require_bit(bit, row=row)
        if bit_value == 1:
            if not 0 <= code < upper:
                raise EncodingRangeError(
                    f"分区 {partition_key} 行 {row}: valid code {code} 超范围 [0, {upper})"
                )
            global_code = local_to_global[code]
            global_codes.append(global_code)
            preserved.append(None)
            logical.append(dictionary.value_of(global_code))
        else:
            global_codes.append(None)
            preserved.append(code if keep_invalid_code else None)
            logical.append(None)
    return RemapResult(
        partition_key=partition_key,
        dictionary_id=dictionary.dictionary_id,
        dictionary_digest=dictionary.dictionary_digest,
        global_codes=global_codes,
        preserved_invalid_codes=preserved,
        logical_values=logical,
        validity=list(validity),
        row_count=len(global_codes),
    )


def remap_batch(
    dictionary: GlobalDictionary | DiskGlobalDictionary,
    partitions: Iterable[Partition | Mapping],
    *,
    keep_invalid_code: bool = True,
) -> RemapBatch:
    """对一组分区重编码；每个分区的列都绑定同一 dictionary_id。"""
    results = []
    for item in partitions:
        partition = _as_partition(item)
        results.append(
            remap_partition(
                dictionary,
                partition.key,
                partition.codes,
                partition.validity,
                keep_invalid_code=keep_invalid_code,
            )
        )
    return RemapBatch(dictionary=dictionary, results=tuple(results))


def stream_remap(
    local_to_global: Sequence[int],
    codes: Iterable[int],
    validity: Iterable[int],
    *,
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
) -> Iterator[tuple[int, int | None]]:
    """大列流式改码：逐行产出 ``(原码, 全局码或 None)``，O(1) 内存。

    ``max_buffer_bytes`` 是消费端批缓冲提示，本函数不缓存行。两侧长度不一致
    时抛 :class:`ValidityError`，绝不静默截断。
    """
    if max_buffer_bytes <= 0:
        raise StreamBufferError("max_buffer_bytes 必须为正")
    upper = len(local_to_global)
    sentinel = object()
    for row, (raw_code, bit) in enumerate(zip_longest(codes, validity, fillvalue=sentinel)):
        if raw_code is sentinel or bit is sentinel:
            raise ValidityError(f"行 {row}: codes 与 validity 长度不一致")
        code = _require_int(raw_code, row=row, field="code")
        bit_value = _require_bit(bit, row=row)
        if bit_value == 1:
            if not 0 <= code < upper:
                raise EncodingRangeError(f"行 {row}: valid code {code} 超范围 [0, {upper})")
            yield code, local_to_global[code]
        else:
            yield code, None


def stream_write_remap(
    destination: Path | str,
    local_to_global: Sequence[int],
    codes: Iterable[int],
    validity: Iterable[int],
    *,
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
) -> int:
    """把重编码列写入 CSV（``global_code,validity``），NULL 写空字段，返回行数。"""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    # 先做长度前检（若两侧都是 Sequence），避免写出残缺列文件。
    if isinstance(codes, Sized) and isinstance(validity, Sized):
        if len(codes) != len(validity):
            raise ValidityError(
                f"codes 长度 {len(codes)} 与 validity 长度 {len(validity)} 不一致"
            )
    with target.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["global_code", "validity"])
        for _, global_code in stream_remap(
            local_to_global, codes, validity, max_buffer_bytes=max_buffer_bytes
        ):
            writer.writerow(
                ["" if global_code is None else str(global_code),
                 "0" if global_code is None else "1"]
            )
            written += 1
    return written


# ---------------------------------------------------------------------------
# 反向语义核对


@dataclass
class VerificationReport:
    ok: bool
    before: dict[str, int]
    after: dict[str, int]
    unexpected: list[str]
    missing: list[str]
    row_order_equal: bool
    null_positions_equal: bool
    errors: list[str]

    def to_json(self) -> dict:
        return {
            "ok": self.ok,
            "before": self.before,
            "after": self.after,
            "unexpected": self.unexpected,
            "missing": self.missing,
            "row_order_equal": self.row_order_equal,
            "null_positions_equal": self.null_positions_equal,
            "errors": self.errors,
        }


def _decode_global_codes(
    global_codes: Sequence[int | None],
    dictionary: GlobalDictionary | DiskGlobalDictionary,
) -> list[str | None]:
    values: list[str | None] = []
    for row, code in enumerate(global_codes):
        if code is None:
            values.append(None)
            continue
        try:
            values.append(dictionary.value_of(code))
        except EncodingRangeError as exc:
            raise EncodingRangeError(f"行 {row}: {exc}") from exc
    return values


def _counts(values: Iterable[str | None]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _decode_local_column(
    codes: Sequence[int],
    validity: Sequence[int],
    local_dictionary: Sequence[str],
    *,
    unicode_normalization: str,
) -> list[str | None]:
    entries = tuple(normalize_entry(item, unicode_normalization) for item in local_dictionary)
    if len(codes) != len(validity):
        raise ValidityError("原始 codes 与 validity 长度不一致")
    values: list[str | None] = []
    for row, (raw_code, bit) in enumerate(zip(codes, validity)):
        code = _require_int(raw_code, row=row, field="code")
        bit_value = _require_bit(bit, row=row)
        if bit_value == 1:
            if not 0 <= code < len(entries):
                raise EncodingRangeError(f"行 {row}: 原始 valid code {code} 超范围")
            values.append(entries[code])
        else:
            values.append(None)
    return values


def verify_semantics(
    result: RemapResult,
    dictionary: GlobalDictionary | DiskGlobalDictionary,
    *,
    original_codes: Sequence[int] | None = None,
    original_validity: Sequence[int] | None = None,
    local_dictionary: Sequence[str] | None = None,
    nfc: bool | None = None,
) -> VerificationReport:
    """反向解码重编码列，与原始本地列逐行、逐多重集核对。

    必须满足：行序不变、NULL 位置不变、valid 逻辑值多重集不变；提供原始列时
    还要求逐行字符串相等（不是 global_code 相等）。``nfc`` 不允许在此临时
    开关，归一化差异必须通过另一个版本字典体现。
    """
    if nfc is not None:
        raise NormalizationMismatchError(
            "verify_semantics 不接受临时 nfc 开关；请用该协议版本的字典核对"
        )
    if dictionary.dictionary_id != result.dictionary_id:
        raise DictionaryVersionError(
            "重编码结果绑定的 dictionary_id 与校验字典不一致；旧列必须用旧字典解释"
        )
    if dictionary.dictionary_digest != result.dictionary_digest:
        raise DictionaryVersionError("重编码结果与校验字典的摘要不一致")

    provided = (
        original_codes is not None,
        original_validity is not None,
        local_dictionary is not None,
    )
    if any(provided) and not all(provided):
        raise DictionaryError(
            "original_codes/original_validity/local_dictionary 必须同时给出或同时省略"
        )
    if original_codes is None:
        before = list(result.logical_values)
    else:
        before = _decode_local_column(
            original_codes,
            original_validity,  # type: ignore[arg-type]
            local_dictionary,  # type: ignore[arg-type]
            unicode_normalization=dictionary.unicode_normalization,
        )
    after = _decode_global_codes(result.global_codes, dictionary)

    errors: list[str] = []
    row_order_equal = len(before) == len(after)
    if not row_order_equal:
        errors.append("重编码前后行数不同")
    row_equal = row_order_equal and all(
        original == decoded for original, decoded in zip(before, after)
    )
    if row_order_equal and not row_equal:
        errors.append("重编码前后逐行逻辑值不同")
    null_positions_equal = row_order_equal and all(
        (original is None) == (decoded is None)
        for original, decoded in zip(before, after)
    )
    if row_order_equal and not null_positions_equal:
        errors.append("重编码前后 NULL 位置不同")

    before_counts = _counts(before)
    after_counts = _counts(after)
    unexpected = sorted(set(after_counts) - set(before_counts))
    missing = sorted(set(before_counts) - set(after_counts))
    if before_counts != after_counts:
        errors.append("逻辑值多重集不同")

    return VerificationReport(
        ok=row_order_equal and row_equal and null_positions_equal and before_counts == after_counts,
        before=dict(sorted(before_counts.items())),
        after=dict(sorted(after_counts.items())),
        unexpected=unexpected,
        missing=missing,
        row_order_equal=row_order_equal,
        null_positions_equal=null_positions_equal,
        errors=errors,
    )


def remap_and_verify(
    dictionary: GlobalDictionary | DiskGlobalDictionary,
    partition: Partition | Mapping,
) -> tuple[RemapResult, VerificationReport]:
    """构建列 -> 重编码 -> 反向核对，一步完成。"""
    item = _as_partition(partition)
    result = remap_partition(dictionary, item.key, item.codes, item.validity)
    report = verify_semantics(
        result,
        dictionary,
        original_codes=item.codes,
        original_validity=item.validity,
        local_dictionary=item.local_dictionary,
    )
    return result, report


# ---------------------------------------------------------------------------
# 追加式稳定编码（可选，排序语义不同）


@dataclass(frozen=True)
class StableDictionary:
    """追加式稳定编码。

    新值只追加到尾部，既有稳定码永不变；代价是编码顺序不再具有 UTF-8 全序
    语义。不要与 :class:`GlobalDictionary` 混用，也不声称同时保证全序与旧码
    不变。
    """

    entries: tuple[str, ...]
    dictionary_id: str
    dictionary_digest: str
    protocol: str = STABLE_PROTOCOL_VERSION
    ordering: str = "append_only_not_lexicographic"

    def index_of(self, value: str) -> int:
        try:
            return self.entries.index(value)
        except ValueError:
            raise KeyError(value) from None

    def to_json(self) -> dict:
        return {
            "protocol": self.protocol,
            "ordering": self.ordering,
            "entries": list(self.entries),
            "dictionary_id": self.dictionary_id,
            "dictionary_digest": self.dictionary_digest,
        }


def stable_codes(
    previous: StableDictionary | Sequence[str] | None,
    new_values: Iterable[str],
) -> StableDictionary:
    """在既有稳定字典尾部追加新值，不改动任何已有码。"""
    if previous is None:
        entries: list[str] = []
    elif isinstance(previous, StableDictionary):
        if previous.protocol != STABLE_PROTOCOL_VERSION:
            raise DictionaryVersionError("既有字典不是追加式稳定协议")
        entries = list(previous.entries)
        if dictionary_digest(entries, protocol=STABLE_PROTOCOL_VERSION) != previous.dictionary_digest:
            raise DictionaryVersionError("既有稳定字典摘要不匹配")
    else:
        entries = list(previous)
        if len(set(entries)) != len(entries):
            raise DuplicateDictionaryEntryError("既有稳定字典含重复项")
    known = set(entries)
    for value in new_values:
        if not isinstance(value, str):
            raise DictionaryError("稳定字典新值必须是字符串")
        if value not in known:
            entries.append(value)
            known.add(value)
    digest = dictionary_digest(entries, protocol=STABLE_PROTOCOL_VERSION)
    return StableDictionary(
        entries=tuple(entries),
        dictionary_id=dictionary_id_for(entries, protocol=STABLE_PROTOCOL_VERSION),
        dictionary_digest=digest,
    )


# ---------------------------------------------------------------------------
# 版本目录与原子发布


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class PublishedVersion:
    dictionary: GlobalDictionary | DiskGlobalDictionary
    directory: Path

    @property
    def entry_count(self) -> int:
        return len(self.dictionary.entries) if isinstance(
            self.dictionary, GlobalDictionary
        ) else len(self.dictionary.file)

    @classmethod
    def load(cls, directory: Path | str) -> "PublishedVersion":
        root = Path(directory)
        if not (root / "COMPLETE").is_file():
            raise ReleaseIncompleteError(f"版本目录未完成发布: {root}")
        data = json.loads((root / "dictionary.json").read_text(encoding="utf-8"))
        if "entries_file" in data:
            dictionary: GlobalDictionary | DiskGlobalDictionary = (
                DiskGlobalDictionary.from_json(data, root)
            )
        else:
            dictionary = GlobalDictionary.from_json(data)
            validate_global_dictionary(dictionary)
        _validate_mappings(dictionary)
        for mapping in dictionary.mappings:
            manifest_path = root / "mappings" / f"{mapping.partition_key}.json"
            if not manifest_path.is_file():
                raise ReleaseIncompleteError(
                    f"版本缺少分区 {mapping.partition_key!r} 的映射文件: {root}"
                )
        return cls(dictionary=dictionary, directory=root)


def _validate_mappings(
    dictionary: GlobalDictionary | DiskGlobalDictionary,
) -> None:
    """公共映射校验，内存/磁盘字典共用。"""
    seen_keys: set[str] = set()
    for mapping in dictionary.mappings:
        if mapping.partition_key in seen_keys:
            raise DictionaryError(f"分区映射 key 重复: {mapping.partition_key!r}")
        seen_keys.add(mapping.partition_key)
        if mapping.dictionary_id != dictionary.dictionary_id:
            raise DictionaryVersionError(
                f"分区 {mapping.partition_key} 映射绑定了其他 dictionary_id"
            )
        if mapping.dictionary_digest != dictionary.dictionary_digest:
            raise DictionaryVersionError(f"分区 {mapping.partition_key} 映射摘要不匹配")
        if mapping.protocol != dictionary.protocol:
            raise NormalizationMismatchError(
                f"分区 {mapping.partition_key} 映射协议与字典不一致"
            )
        if len(mapping.local_to_global) != len(mapping.local_dictionary):
            raise DictionaryError(
                f"分区 {mapping.partition_key}: 转换表长度与本地字典不一致"
            )


def _validate_disk_dictionary(dictionary: DiskGlobalDictionary) -> None:
    if len(dictionary.file) == 0:
        raise DictionaryError("公共字典为空")
    if dictionary.protocol != protocol_for(dictionary.unicode_normalization):
        raise NormalizationMismatchError("字典协议与归一化选项不匹配")
    actual = dictionary.file.digest(protocol=dictionary.protocol)
    if actual != dictionary.dictionary_digest:
        raise DictionaryVersionError("磁盘字典摘要与 entries 不匹配")
    if dictionary.dictionary_id != f"{dictionary.protocol.split('/')[0]}-{actual[:16]}":
        raise DictionaryVersionError("dictionary_id 与 entries 不匹配")
    _validate_mappings(dictionary)
    upper = len(dictionary.file)
    for mapping in dictionary.mappings:
        for code in mapping.local_to_global:
            if not 0 <= code < upper:
                raise DictionaryVersionError(
                    f"分区 {mapping.partition_key}: 转换表指向越界 global_code {code}"
                )
        for local_code, global_code in enumerate(mapping.local_to_global):
            if dictionary.file.value_of(global_code) != mapping.local_dictionary[local_code]:
                raise DictionaryVersionError(
                    f"分区 {mapping.partition_key}: 转换表未正确指向公共字典"
                )


class Publisher:
    """原子发布公共字典 + 全部目标列映射 + 重编码列。

    布局::

        <root>/versions/<dictionary_id>/dictionary.json
        <root>/versions/<dictionary_id>/mappings/<partition>.json
        <root>/versions/<dictionary_id>/columns/<partition>.csv
        <root>/versions/<dictionary_id>/columns/<partition>.json   # 列清单
        <root>/versions/<dictionary_id>/COMPLETE        # 最后写入
        <root>/CURRENT -> versions/<dictionary_id>      # 全部写完后原子切换

    未完成时 ``COMPLETE`` 不存在，旧读者继续读取 ``CURRENT`` 指向的旧版本。
    既有版本目录不会被覆盖；同 id 但内容不同则拒绝（新内容必须换版本）。
    """

    def __init__(self, root: Path | str):
        self.root = Path(root)

    @property
    def versions_dir(self) -> Path:
        return self.root / "versions"

    @property
    def current_link(self) -> Path:
        return self.root / "CURRENT"

    def list_versions(self) -> list[str]:
        if not self.versions_dir.is_dir():
            return []
        return sorted(
            entry.name
            for entry in self.versions_dir.iterdir()
            if entry.is_dir() and not entry.name.startswith(".")
        )

    def load_version(self, dictionary_id: str) -> PublishedVersion:
        return PublishedVersion.load(self.versions_dir / dictionary_id)

    def current(self) -> PublishedVersion | None:
        link = self.current_link
        if not os.path.lexists(link):
            return None
        try:
            target = Path(os.readlink(link))
        except OSError:
            target = link
        if not target.is_absolute():
            target = self.root / target
        return PublishedVersion.load(target)

    def publish(
        self,
        dictionary: GlobalDictionary | DiskGlobalDictionary,
        columns: Mapping[str, tuple[Sequence[int], Sequence[int]]] | None = None,
        *,
        max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
    ) -> PublishedVersion:
        """发布新版本；全部目标列写完并标记完成后才切换 CURRENT。

        追加式内容变化会产生新的 dictionary_id；此方法拒绝原地替换已有版本。
        """
        if isinstance(dictionary, GlobalDictionary):
            validate_global_dictionary(dictionary)
        else:
            _validate_disk_dictionary(dictionary)
        previous = self.current()
        if previous is not None and dictionary.supersedes is None:
            if previous.dictionary.dictionary_id != dictionary.dictionary_id:
                dictionary = replace(dictionary, supersedes=previous.dictionary.dictionary_id)

        version_dir = self.versions_dir / dictionary.dictionary_id
        if version_dir.exists():
            try:
                existing = PublishedVersion.load(version_dir)
            except ReleaseIncompleteError:
                if self._current_id() == dictionary.dictionary_id:
                    raise
                # 上次发布中途失败留下的未完成目录：没有读者能看到它，安全清除。
                shutil.rmtree(version_dir)
                existing = None
            if existing is not None:
                if (
                    existing.dictionary.dictionary_digest != dictionary.dictionary_digest
                    or existing.entry_count != (
                        len(dictionary.entries)
                        if isinstance(dictionary, GlobalDictionary)
                        else len(dictionary.file)
                    )
                ):
                    raise DictionaryVersionError(
                        f"dictionary_id {dictionary.dictionary_id} 已存在且内容不同；"
                        "新内容必须使用新版本，不能原地替换"
                    )
                missing = [
                    key
                    for key in (columns or {})
                    if not (version_dir / "columns" / f"{key}.json").is_file()
                ]
                if missing:
                    raise DictionaryVersionError(
                        f"版本 {dictionary.dictionary_id} 已完成发布但缺少列 {missing}；"
                        "不能向已完成版本追加内容"
                    )
                self._activate(version_dir)
                return PublishedVersion.load(version_dir)
        self._write_and_install(
            self.versions_dir / f".staging-{dictionary.dictionary_id}-{os.getpid()}",
            version_dir,
            dictionary,
            columns,
            max_buffer_bytes,
        )

        self._activate(version_dir)
        return PublishedVersion.load(version_dir)

    def _current_id(self) -> str | None:
        version = self.current()
        return None if version is None else version.dictionary.dictionary_id

    def _write_and_install(
        self,
        staging: Path,
        version_dir: Path,
        dictionary: GlobalDictionary | DiskGlobalDictionary,
        columns: Mapping[str, tuple[Sequence[int], Sequence[int]]] | None,
        max_buffer_bytes: int,
    ) -> None:
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        try:
            self._write_version(staging, dictionary, columns or {}, max_buffer_bytes)
            if version_dir.exists():
                raise DictionaryVersionError(
                    f"dictionary_id {dictionary.dictionary_id} 在发布期间被创建"
                )
            os.rename(staging, version_dir)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def _write_version(
        self,
        directory: Path,
        dictionary: GlobalDictionary | DiskGlobalDictionary,
        columns: Mapping[str, tuple[Sequence[int], Sequence[int]]],
        max_buffer_bytes: int,
    ) -> None:
        if isinstance(dictionary, DiskGlobalDictionary):
            # 字典项文件复制进版本目录，版本自包含；稀疏索引一并复制。
            entries_dir = directory / "entries"
            entries_dir.mkdir()
            shutil.copyfile(dictionary.file.data_path, entries_dir / "entries.jsonl")
            shutil.copyfile(
                dictionary.file.index_path, entries_dir / "entries.jsonl.idx"
            )
        (directory / "dictionary.json").write_text(
            json.dumps(dictionary.to_json(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        mappings_dir = directory / "mappings"
        mappings_dir.mkdir()
        for mapping in dictionary.mappings:
            (mappings_dir / f"{mapping.partition_key}.json").write_text(
                json.dumps(mapping.to_json(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        columns_dir = directory / "columns"
        columns_dir.mkdir()
        known_partitions = {mapping.partition_key for mapping in dictionary.mappings}
        for key, (codes, validity) in columns.items():
            if key not in known_partitions:
                raise DictionaryError(f"目标列分区 {key!r} 不在本版本映射中")
            mapping = dictionary.mapping_for(key)
            _check_mapping_binding(dictionary, mapping)
            if len(codes) != len(validity):
                raise ValidityError(f"分区 {key}: codes 与 validity 长度不一致")
            csv_path = columns_dir / f"{key}.csv"
            row_count = stream_write_remap(
                csv_path,
                mapping.local_to_global,
                codes,
                validity,
                max_buffer_bytes=max_buffer_bytes,
            )
            (columns_dir / f"{key}.json").write_text(
                json.dumps(
                    {
                        "partition_key": key,
                        "dictionary_id": dictionary.dictionary_id,
                        "dictionary_digest": dictionary.dictionary_digest,
                        "null_encoding": NULL_ENCODING,
                        "row_count": row_count,
                        "sha256": _sha256_file(csv_path),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        # 完成标记最后写；此前读者看不到 COMPLETE。
        (directory / "COMPLETE").write_text(
            json.dumps({"__complete__": True, "dictionary_id": dictionary.dictionary_id}) + "\n",
            encoding="utf-8",
        )

    def _activate(self, version_dir: Path) -> None:
        target = Path("versions") / version_dir.name
        temporary = self.root / f".CURRENT-{os.getpid()}"
        if os.path.lexists(temporary):
            temporary.unlink()
        os.symlink(target, temporary)
        os.replace(temporary, self.current_link)


def read_published_column(
    version: PublishedVersion,
    partition_key: str,
) -> tuple[list[int | None], list[int]]:
    """按版本读取已发布重编码列，返回 (global_codes, validity)。

    会核对列清单里的 dictionary_id/摘要与 CSV 摘要；旧快照必须通过它自己的
    ``PublishedVersion`` 读取，从而按旧字典解释。
    """
    directory = version.directory / "columns"
    manifest_path = directory / f"{partition_key}.json"
    csv_path = directory / f"{partition_key}.csv"
    if not manifest_path.is_file() or not csv_path.is_file():
        raise FileNotFoundError(f"版本中没有分区 {partition_key!r} 的完整列: {version.directory}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest["dictionary_id"] != version.dictionary.dictionary_id:
        raise DictionaryVersionError("列清单绑定的 dictionary_id 与版本不一致")
    if manifest["dictionary_digest"] != version.dictionary.dictionary_digest:
        raise DictionaryVersionError("列清单绑定的字典摘要与版本不一致")
    if manifest["sha256"] != _sha256_file(csv_path):
        raise DictionaryVersionError(f"分区 {partition_key!r} 列内容摘要不匹配")

    codes: list[int | None] = []
    validity: list[int] = []
    with csv_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        for line, row in enumerate(reader, start=2):
            bit = row["validity"]
            if bit not in ("0", "1"):
                raise ValidityError(f"{csv_path} 行 {line}: validity 必须是 0 或 1")
            bit_value = int(bit)
            raw = row["global_code"]
            if raw == "":
                code: int | None = None
            else:
                try:
                    code = int(raw)
                except ValueError:
                    raise DictionaryError(
                        f"{csv_path} 行 {line}: global_code 不是整数: {raw!r}"
                    ) from None
            if (code is None) != (bit_value == 0):
                raise DictionaryError(
                    f"{csv_path} 行 {line}: global_code 空值与 validity 不一致"
                )
            codes.append(code)
            validity.append(bit_value)
    if len(codes) != manifest["row_count"]:
        raise DictionaryVersionError("列行数与清单不一致")
    return codes, validity


def decode_published_column(
    version: PublishedVersion,
    partition_key: str,
) -> list[str | None]:
    """把发布列解码回逻辑值；可用旧版本对象复核旧快照的解释。"""
    codes, _ = read_published_column(version, partition_key)
    return _decode_global_codes(codes, version.dictionary)


# ---------------------------------------------------------------------------
# 构建报告与命令行


def build_report(
    document: Mapping,
    *,
    include_append: bool = False,
    unicode_normalization: str = "none",
    max_buffer_bytes: int | None = None,
) -> dict:
    """生成可直接写入 .workspace-output 的构建报告（不覆盖原始资料）。

    ``max_buffer_bytes`` 非空时用外部归并排序构建公共字典。
    """
    partitions = partitions_from_document(document, include_append=include_append)
    dictionary = build_global_dictionary(
        partitions,
        unicode_normalization=unicode_normalization,
        max_buffer_bytes=max_buffer_bytes,
    )
    rows = []
    for partition in partitions:
        result, report = remap_and_verify(dictionary, partition)
        rows.append(
            {
                "partition_key": partition.key,
                "dictionary_id": result.dictionary_id,
                "dictionary_digest": result.dictionary_digest,
                "global_codes": result.global_codes,
                "preserved_invalid_codes": result.preserved_invalid_codes,
                "logical_values": result.logical_values,
                "logical_multiset": result.logical_multiset,
                "verification": report.to_json(),
            }
        )
    return {
        "protocol": dictionary.protocol,
        "unicode_normalization": dictionary.unicode_normalization,
        "null_encoding": NULL_ENCODING,
        "dictionary": dictionary.to_json(),
        "partitions": rows,
    }


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="构建分区公共字典、重编码列并做反向语义核对"
    )
    parser.add_argument("partitions", help="分区 JSON 路径")
    parser.add_argument("--include-append", action="store_true", help="把 append_partition 一起构建")
    parser.add_argument("--nfc", action="store_true", help="使用 NFC 归一化协议（另一个版本）")
    parser.add_argument("--output", help="输出报告 JSON 路径（默认 stdout）")
    parser.add_argument("--publish", help="发布到该版本目录根")
    parser.add_argument(
        "--max-buffer-bytes",
        type=int,
        help="非空时用外部归并排序构建公共字典（有界内存）",
    )
    args = parser.parse_args(argv)

    document = json.loads(Path(args.partitions).read_text(encoding="utf-8"))
    normalization = "nfc" if args.nfc else "none"
    report = build_report(
        document,
        include_append=args.include_append,
        unicode_normalization=normalization,
        max_buffer_bytes=args.max_buffer_bytes,
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    if args.publish:
        dictionary = GlobalDictionary.from_json(report["dictionary"])
        partitions = partitions_from_document(document, include_append=args.include_append)
        Publisher(args.publish).publish(
            dictionary,
            {part.key: (part.codes, part.validity) for part in partitions},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
