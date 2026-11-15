import json
import shutil
import tempfile
import unicodedata
import unittest
from pathlib import Path

from src.dictionary_codec import (
    DictionaryError,
    DictionaryVersionError,
    DuplicateDictionaryEntryError,
    EncodingRangeError,
    NormalizationMismatchError,
    Partition,
    Publisher,
    PublishedVersion,
    ReleaseIncompleteError,
    ValidityError,
    build_from_partitions_document,
    build_global_dictionary,
    build_global_dictionary_streaming,
    build_disk_dictionary,
    decode_published_column,
    external_sort_entries,
    external_sort_to_file,
    partitions_from_document,
    read_published_column,
    remap_and_verify,
    remap_batch,
    remap_partition,
    stable_codes,
    stream_remap,
    verify_semantics,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


def fixture_document():
    return json.loads((FIXTURES / "partitions.json").read_text(encoding="utf-8"))


def base_partitions(document):
    return partitions_from_document(document)


def appended_partition(document):
    return Partition.from_json(document["append_partition"])


class GlobalDictionaryTests(unittest.TestCase):
    def test_public_dictionary_sorted_by_utf8_bytes(self):
        dictionary = build_from_partitions_document(fixture_document())
        self.assertEqual(dictionary.entries, ("alpha", "beta", "gamma"))

    def test_explicit_mappings_not_numeric_merge(self):
        dictionary = build_from_partitions_document(fixture_document())
        self.assertEqual(dictionary.mapping_for("P").local_to_global, (1, 0))
        self.assertEqual(dictionary.mapping_for("Q").local_to_global, (0, 2))

    def test_partition_order_does_not_change_dictionary(self):
        document = fixture_document()
        forward = build_global_dictionary(base_partitions(document))
        reverse = build_global_dictionary(list(reversed(base_partitions(document))))
        self.assertEqual(forward.entries, reverse.entries)
        self.assertEqual(forward.dictionary_id, reverse.dictionary_id)
        self.assertEqual(forward.dictionary_digest, reverse.dictionary_digest)

    def test_non_ascii_utf8_order(self):
        # 多字节字符按 UTF-8 首字节定位：'A' < 'a' < '中'(E4) < '𝄞'(F0)。
        values = ["中", "a", "𝄞", "A"]
        dictionary = build_global_dictionary(
            [Partition("N", values, [0, 1, 2, 3], [1, 1, 1, 1])]
        )
        self.assertEqual(dictionary.entries, ("A", "a", "中", "𝄞"))
        self.assertEqual(
            dictionary.entries, tuple(sorted(values, key=lambda s: s.encode("utf-8")))
        )

    def test_utf8_byte_order_is_length_independent(self):
        # U+10000（4 字节，F0 90 80 80）与 U+FFFD（3 字节，EF BF BD）：
        # 码点序 U+10000 > U+FFFD 且字节序同样如此；但按 UTF-16 单元序会反转。
        # 库必须按 UTF-8 字节而非 UTF-16/本地化顺序排序。
        values = ["\U00010000", "�"]
        self.assertGreater(
            values[0].encode("utf-8"), values[1].encode("utf-8")
        )
        self.assertLess(values[0].encode("utf-16-be"), values[1].encode("utf-16-be"))
        dictionary = build_global_dictionary([Partition("N", values, [0, 1], [1, 1])])
        self.assertEqual(dictionary.entries, ("�", "\U00010000"))
        self.assertEqual(dictionary.mapping_for("N").local_to_global, (1, 0))

    def test_empty_string_is_a_value_never_null(self):
        document = fixture_document()
        dictionary = build_from_partitions_document(document, include_append=True)
        self.assertIn("", dictionary.entries)
        self.assertEqual(remap_partition(dictionary, "R", [0, 1], [1, 1]).global_codes, [1, 0])

    def test_dictionary_duplicate_rejected(self):
        with self.assertRaises(DuplicateDictionaryEntryError):
            build_global_dictionary([Partition("P", ["a", "a"], [0], [1])])

    def test_bytes_dictionary_rejected(self):
        with self.assertRaises(DictionaryError):
            build_global_dictionary([Partition("P", [b"a"], [0], [1])])

    def test_duplicate_partition_key_rejected(self):
        with self.assertRaises(DictionaryError):
            build_global_dictionary(
                [Partition("P", ["a"], [0], [1]), Partition("P", ["b"], [0], [1])]
            )

    def test_nfc_is_a_separate_protocol_version(self):
        composed = unicodedata.normalize("NFC", "café")
        decomposed = unicodedata.normalize("NFD", "café")
        self.assertNotEqual(composed, decomposed)
        parts = [
            Partition("A", [composed], [0], [1]),
            Partition("B", [decomposed], [0], [1]),
        ]
        exact = build_global_dictionary(parts)
        self.assertEqual(len(exact.entries), 2)
        self.assertNotEqual(exact.protocol, build_global_dictionary(parts, unicode_normalization="nfc").protocol)

        merged = build_global_dictionary(parts, unicode_normalization="nfc")
        self.assertEqual(len(merged.entries), 1)
        self.assertEqual(merged.mapping_for("A").local_to_global, (0,))
        self.assertEqual(merged.mapping_for("B").local_to_global, (0,))
        # 另一个协议版本，不能复用 none 版本的 id 或摘要。
        self.assertNotEqual(merged.dictionary_id, exact.dictionary_id)
        self.assertNotEqual(merged.dictionary_digest, exact.dictionary_digest)
        self.assertEqual(merged.unicode_normalization, "nfc")

    def test_nfc_collision_inside_one_local_dictionary_rejected(self):
        composed = unicodedata.normalize("NFC", "café")
        decomposed = unicodedata.normalize("NFD", "café")
        with self.assertRaises(DuplicateDictionaryEntryError):
            build_global_dictionary(
                [Partition("A", [composed, decomposed], [0, 1], [1, 1])],
                unicode_normalization="nfc",
            )
        # 默认协议下二者是不同的本地值，应被允许。
        exact = build_global_dictionary(
            [Partition("A", [composed, decomposed], [0, 1], [1, 1])]
        )
        self.assertEqual(len(exact.entries), 2)

    def test_protocol_override_mismatch_rejected(self):
        with self.assertRaises(NormalizationMismatchError):
            build_global_dictionary(
                [Partition("P", ["a"], [0], [1])],
                unicode_normalization="nfc",
                protocol="exact_utf8_bytes_sorted/v1",
            )


class RemapTests(unittest.TestCase):
    def setUp(self):
        self.document = fixture_document()
        self.dictionary = build_from_partitions_document(self.document)

    def test_remap_matches_expected_mapping(self):
        result = remap_partition(self.dictionary, "P", [0, 1, 0], [1, 1, 0])
        self.assertEqual(result.global_codes, [1, 0, None])
        self.assertEqual(result.preserved_invalid_codes, [None, None, 0])
        self.assertEqual(result.logical_values, ["beta", "alpha", None])

    def test_null_is_not_minus_one(self):
        result = remap_partition(self.dictionary, "P", [0, -1, 1], [1, 0, 1])
        self.assertEqual(result.global_codes, [1, None, 0])
        # 无效位置的 -1 原样保留，不被当成 NULL 解码成某个字典值。
        self.assertEqual(result.preserved_invalid_codes, [None, -1, None])
        self.assertEqual(result.logical_values, ["beta", None, "alpha"])

    def test_invalid_original_code_preserved_but_not_semantic(self):
        document = json.loads(json.dumps(self.document))
        document["partitions"][0]["codes"] = [0, 1, 7]
        document["partitions"][0]["validity"] = [1, 1, 0]
        dictionary = build_from_partitions_document(document)
        result = remap_partition(dictionary, "P", [0, 1, 7], [1, 1, 0])
        self.assertEqual(result.preserved_invalid_codes, [None, None, 7])
        self.assertIsNone(result.global_codes[2])
        self.assertIsNone(result.logical_values[2])

    def test_invalid_code_out_of_range_allowed(self):
        result = remap_partition(self.dictionary, "P", [0, 999999], [1, 0])
        self.assertEqual(result.global_codes, [1, None])
        self.assertEqual(result.preserved_invalid_codes, [None, 999999])

    def test_bad_valid_code_rejected(self):
        with self.assertRaises(EncodingRangeError):
            remap_partition(self.dictionary, "P", [0, 5], [1, 1])

    def test_negative_valid_code_rejected(self):
        with self.assertRaises(EncodingRangeError):
            remap_partition(self.dictionary, "P", [-1], [1])

    def test_validity_must_be_zero_or_one(self):
        with self.assertRaises(ValidityError):
            remap_partition(self.dictionary, "P", [0], [2])

    def test_validity_length_mismatch(self):
        with self.assertRaises(ValidityError):
            remap_partition(self.dictionary, "P", [0, 1], [1])

    def test_boolean_code_rejected(self):
        with self.assertRaises(DictionaryError):
            remap_partition(self.dictionary, "P", [True], [1])

    def test_same_value_across_partitions_gets_same_global_code(self):
        result_p = remap_partition(self.dictionary, "P", [0], [1])
        result_q = remap_partition(self.dictionary, "Q", [0], [1])
        self.assertEqual(result_p.logical_values, ["beta"])
        self.assertEqual(result_q.logical_values, ["alpha"])
        # alpha 在 P 是本地 1、在 Q 是本地 0，必须映射到同一 global_code。
        self.assertEqual(
            remap_partition(self.dictionary, "P", [1], [1]).global_codes,
            remap_partition(self.dictionary, "Q", [0], [1]).global_codes,
        )

    def test_row_order_and_multiset_preserved(self):
        codes = [0, 1, 0, 1, 0]
        validity = [1, 1, 0, 1, 1]
        result, report = remap_and_verify(
            self.dictionary, Partition("P", ["beta", "alpha"], codes, validity)
        )
        self.assertTrue(report.ok, report.errors)
        self.assertEqual(result.logical_values, ["beta", "alpha", None, "alpha", "beta"])
        self.assertEqual(report.before, {"alpha": 2, "beta": 2})
        self.assertEqual(report.after, {"alpha": 2, "beta": 2})
        self.assertTrue(report.row_order_equal)
        self.assertTrue(report.null_positions_equal)

    def test_verify_detects_code_swap(self):
        result = remap_partition(self.dictionary, "P", [0, 1], [1, 1])
        # 故意换成“本地数字直拼”的结果：0/1 在全局字典下含义恰好互换。
        result.logical_values = ["alpha", "beta"]
        result.global_codes = [0, 1]
        report = verify_semantics(
            result,
            self.dictionary,
            original_codes=[0, 1],
            original_validity=[1, 1],
            local_dictionary=["beta", "alpha"],
        )
        self.assertFalse(report.ok)
        self.assertEqual(report.before, {"alpha": 1, "beta": 1})
        self.assertEqual(report.after, {"alpha": 1, "beta": 1})
        self.assertTrue(report.row_order_equal)
        self.assertTrue(report.null_positions_equal)
        self.assertIn("重编码前后逐行逻辑值不同", report.errors)

    def test_verify_rejects_wrong_dictionary_version(self):
        result = remap_partition(self.dictionary, "P", [0], [1])
        newer = build_from_partitions_document(self.document, include_append=True)
        with self.assertRaises(DictionaryVersionError):
            verify_semantics(result, newer)

    def test_verify_rejects_ad_hoc_nfc_switch(self):
        result = remap_partition(self.dictionary, "P", [0], [1])
        with self.assertRaises(NormalizationMismatchError):
            verify_semantics(result, self.dictionary, nfc=True)

    def test_batch_binds_one_version(self):
        batch = remap_batch(self.dictionary, base_partitions(self.document))
        self.assertEqual({r.dictionary_id for r in batch.results}, {self.dictionary.dictionary_id})
        self.assertEqual(batch.for_partition("Q").global_codes, [0, 2])


class AppendVersionTests(unittest.TestCase):
    def setUp(self):
        self.document = fixture_document()
        self.base = build_from_partitions_document(self.document)
        self.with_append = build_from_partitions_document(self.document, include_append=True)

    def test_append_changes_dictionary_version(self):
        self.assertNotEqual(self.base.dictionary_id, self.with_append.dictionary_id)
        self.assertNotEqual(self.base.dictionary_digest, self.with_append.dictionary_digest)

    def test_append_shifts_existing_global_codes(self):
        old = remap_partition(self.base, "P", [0, 1], [1, 1])
        new = remap_partition(self.with_append, "P", [0, 1], [1, 1])
        self.assertEqual(old.global_codes, [1, 0])
        self.assertEqual(new.global_codes, [3, 2])
        self.assertEqual(old.logical_values, new.logical_values)

    def test_aardvark_appended_before_existing_entries(self):
        # 加入 aardvark 后旧 global_code 全部 +1。base 中 P 的码是 [1, 0]，
        # 追加后是 [3, 2]；Q 的 alpha 从 0 变 2，因为 '' 也进入公共字典。
        appended = build_from_partitions_document(
            self.document, include_append=True
        )
        self.assertEqual(appended.entries, ("", "aardvark", "alpha", "beta", "gamma"))
        base_alpha = remap_partition(self.base, "Q", [0], [1]).global_codes[0]
        appended_alpha = remap_partition(appended, "Q", [0], [1]).global_codes[0]
        self.assertEqual(base_alpha, 0)
        self.assertEqual(appended_alpha, 2)
        # 仅追加含 aardvark 的纯值分区时，alpha/beta 各恰好 +1。
        only_aardvark = build_global_dictionary(
            base_partitions(self.document) + [Partition("S", ["aardvark"], [0], [1])]
        )
        self.assertEqual(
            remap_partition(only_aardvark, "P", [0, 1], [1, 1]).global_codes, [2, 1]
        )

    def test_supersedes_links_previous_version(self):
        first = build_global_dictionary(base_partitions(self.document))
        second = build_global_dictionary(
            base_partitions(self.document) + [appended_partition(self.document)],
            supersedes=first.dictionary_id,
        )
        self.assertEqual(second.supersedes, first.dictionary_id)

    def test_stable_codes_keep_old_codes_but_lose_sort_order(self):
        stable = stable_codes(None, ["beta", "alpha"])
        self.assertEqual(stable.entries, ("beta", "alpha"))
        extended = stable_codes(stable, ["aardvark"])
        self.assertEqual(extended.entries, ("beta", "alpha", "aardvark"))
        self.assertNotEqual(extended.entries, tuple(sorted(extended.entries, key=lambda s: s.encode("utf-8"))))
        self.assertEqual(extended.index_of("beta"), 0)
        self.assertEqual(extended.index_of("alpha"), 1)
        self.assertEqual(extended.index_of("aardvark"), 2)


def replace_digest(dictionary):
    from dataclasses import replace

    return replace(dictionary, dictionary_digest="0" * 64)


class StreamingTests(unittest.TestCase):
    def test_stream_remap_is_lazy_and_ordered(self):
        dictionary = build_from_partitions_document(fixture_document())
        mapping = dictionary.mapping_for("P")
        pairs = list(stream_remap(mapping.local_to_global, [0, 1, 0], [1, 1, 0]))
        self.assertEqual(pairs, [(0, 1), (1, 0), (0, None)])

    def test_stream_remap_does_not_consume_all_first(self):
        consumed = []

        def codes():
            for index in range(5):
                consumed.append(index)
                yield index % 2

        iterator = stream_remap([0, 1], codes(), [1] * 5)
        next(iterator)
        self.assertLess(len(consumed), 5)

    def test_external_sort_matches_in_memory_order(self):
        values = ["中", "a", "𝄞", "", "aa", "a"]
        expected = tuple(sorted(set(values), key=lambda s: s.encode("utf-8")))
        self.assertEqual(
            external_sort_entries(values, max_buffer_bytes=1), expected
        )
        self.assertEqual(
            external_sort_entries(iter(values), max_buffer_bytes=4), expected
        )

    def test_streaming_build_matches_in_memory_build(self):
        document = fixture_document()
        parts = base_partitions(document)
        in_memory = build_global_dictionary(parts)
        streamed = build_global_dictionary_streaming(
            lambda: base_partitions(document), max_buffer_bytes=4
        )
        self.assertEqual(streamed.entries, in_memory.entries)
        self.assertEqual(streamed.dictionary_id, in_memory.dictionary_id)
        for mapping in in_memory.mappings:
            self.assertEqual(
                streamed.mapping_for(mapping.partition_key).local_to_global,
                mapping.local_to_global,
            )

    def test_external_sort_rejects_non_strings(self):
        with self.assertRaises(DictionaryError):
            external_sort_entries(["a", 1], max_buffer_bytes=2)

    def test_disk_dictionary_random_access_matches_memory(self):
        values = [f"值{i:03d}" if i % 3 else "" for i in range(200)]
        expected = sorted(set(values), key=lambda s: s.encode("utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            disk = external_sort_to_file(
                iter(values),
                Path(directory) / "dictionary.sorted",
                max_buffer_bytes=7,
                index_stride=3,
            )
            self.assertEqual(disk.materialize(), tuple(expected))
            self.assertEqual(len(disk), len(expected))
            for code, entry in enumerate(expected):
                self.assertEqual(disk.index_of(entry), code)
                self.assertEqual(disk.value_of(code), entry)
            for missing in ("absent", "值999", "a"):
                if missing not in expected:
                    with self.assertRaises(KeyError):
                        disk.index_of(missing)

    def test_disk_dictionary_digest_matches_memory_digest(self):
        values = ["a", "b", "中", ""]
        with tempfile.TemporaryDirectory() as directory:
            disk = external_sort_to_file(values, Path(directory) / "entries")
            from src.dictionary_codec import PROTOCOL_VERSION, dictionary_digest

            self.assertEqual(
                disk.digest(protocol=PROTOCOL_VERSION),
                dictionary_digest(disk.materialize(), protocol=PROTOCOL_VERSION),
            )

    def test_disk_dictionary_build_and_remap(self):
        document = fixture_document()
        with tempfile.TemporaryDirectory() as directory:
            disk = build_disk_dictionary(
                lambda: base_partitions(document),
                Path(directory) / "global",
                max_buffer_bytes=4,
                index_stride=1,
            )
            self.assertEqual(disk.materialize_entries(), ("alpha", "beta", "gamma"))
            self.assertEqual(disk.mapping_for("P").local_to_global, (1, 0))
            result, report = remap_and_verify(
                disk, Partition("P", ["beta", "alpha"], [0, 1, 0], [1, 1, 0])
            )
            self.assertTrue(report.ok, report.errors)
            self.assertEqual(result.global_codes, [1, 0, None])

    def test_disk_dictionary_publish_and_reload(self):
        document = fixture_document()
        parts = base_partitions(document)
        with tempfile.TemporaryDirectory() as directory:
            disk = build_disk_dictionary(
                lambda: base_partitions(document),
                Path(directory) / "global",
                max_buffer_bytes=4,
                index_stride=1,
            )
            publisher = Publisher(Path(directory) / "release")
            version = publisher.publish(
                disk, {p.key: (p.codes, p.validity) for p in parts}
            )
            reloaded = publisher.current()
            self.assertEqual(reloaded.dictionary.dictionary_id, disk.dictionary_id)
            self.assertEqual(decode_published_column(reloaded, "P"), ["beta", "alpha", None])
            # 发布后的磁盘字典可继续独立随机查找。
            self.assertEqual(reloaded.dictionary.value_of(1), "beta")
            self.assertEqual(reloaded.dictionary.index_of("gamma"), 2)
            self.assertEqual(
                (version.directory / "entries" / "entries.jsonl").is_file(), True
            )

    def test_stream_remap_rejects_length_mismatch(self):
        with self.assertRaises(ValidityError):
            list(stream_remap([0, 1], iter([0, 1]), iter([1])))

    def test_stream_remap_rejects_trailing_rows(self):
        with self.assertRaises(ValidityError):
            list(stream_remap([0, 1], iter([0, 1, 0]), iter([1, 1])))


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.directory, True)
        self.document = fixture_document()
        self.partitions = base_partitions(self.document)
        self.dictionary = build_global_dictionary(self.partitions)
        self.columns = {p.key: (p.codes, p.validity) for p in self.partitions}

    def test_publish_and_read_back(self):
        publisher = Publisher(self.directory)
        version = publisher.publish(self.dictionary, self.columns)
        self.assertEqual(publisher.current().dictionary.dictionary_id, version.dictionary.dictionary_id)
        self.assertEqual(decode_published_column(version, "P"), ["beta", "alpha", None])
        self.assertEqual(decode_published_column(version, "Q"), ["alpha", "gamma"])

    def test_new_version_does_not_change_old_snapshot(self):
        publisher = Publisher(self.directory)
        first = publisher.publish(self.dictionary, self.columns)
        old_bytes = (first.directory / "dictionary.json").read_bytes()

        appended = appended_partition(self.document)
        new_dictionary = build_global_dictionary(self.partitions + [appended])
        second = publisher.publish(
            new_dictionary,
            {**self.columns, appended.key: (appended.codes, appended.validity)},
        )
        self.assertNotEqual(first.dictionary.dictionary_id, second.dictionary.dictionary_id)
        self.assertEqual(second.dictionary.supersedes, first.dictionary.dictionary_id)
        # 旧版本文件未被改写；旧列仍按旧字典解释，得到同样逻辑值。
        self.assertEqual((first.directory / "dictionary.json").read_bytes(), old_bytes)
        self.assertEqual(decode_published_column(first, "P"), ["beta", "alpha", None])
        self.assertEqual(decode_published_column(second, "P"), ["beta", "alpha", None])
        # 码值本身已平移。
        self.assertEqual(read_published_column(first, "P")[0], [1, 0, None])
        self.assertEqual(read_published_column(second, "P")[0], [3, 2, None])

    def test_in_place_dictionary_replacement_rejected(self):
        publisher = Publisher(self.directory)
        first = publisher.publish(self.dictionary, self.columns)
        torn = replace_digest(self.dictionary)
        with self.assertRaises(DictionaryVersionError):
            publisher.publish(torn, self.columns)
        self.assertEqual(publisher.current().dictionary.dictionary_id, first.dictionary.dictionary_id)

    def test_incomplete_release_does_not_show_as_current(self):
        publisher = Publisher(self.directory)
        publisher.publish(self.dictionary, self.columns)
        current_id = publisher.current().dictionary.dictionary_id

        # 模拟另一次发布在写出 COMPLETE 前中断。
        broken_dir = publisher.versions_dir / "exact_utf8_bytes_sorted-broken00000000"
        (broken_dir / "mappings").mkdir(parents=True)
        (broken_dir / "dictionary.json").write_text(
            json.dumps(self.dictionary.to_json(), ensure_ascii=False), encoding="utf-8"
        )
        with self.assertRaises(ReleaseIncompleteError):
            PublishedVersion.load(broken_dir)
        self.assertEqual(publisher.current().dictionary.dictionary_id, current_id)

    def test_interrupted_publish_leaves_old_reader_intact_and_is_recoverable(self):
        publisher = Publisher(self.directory)
        first = publisher.publish(self.dictionary, self.columns)

        appended = appended_partition(self.document)
        new_dictionary = build_global_dictionary(self.partitions + [appended])
        new_columns = {**self.columns, appended.key: (appended.codes, appended.validity)}

        original_write_version = Publisher._write_version

        def failing_write(self_, directory, dictionary, columns, max_buffer_bytes):
            original_write_version(self_, directory, dictionary, columns, max_buffer_bytes)
            raise OSError("模拟磁盘中断")

        Publisher._write_version = failing_write
        try:
            with self.assertRaises(OSError):
                publisher.publish(new_dictionary, new_columns)
        finally:
            Publisher._write_version = original_write_version

        # 旧读者完全不受影响。
        self.assertEqual(publisher.current().dictionary.dictionary_id, first.dictionary.dictionary_id)
        self.assertEqual(decode_published_column(publisher.current(), "P"), ["beta", "alpha", None])
        # 新版目录尚未出现在版本列表中（只有暂存目录残留）。
        self.assertNotIn(new_dictionary.dictionary_id, publisher.list_versions())

        # 恢复：重试发布成功，旧版本仍可读。
        second = publisher.publish(new_dictionary, new_columns)
        self.assertEqual(second.dictionary.dictionary_id, new_dictionary.dictionary_id)
        self.assertEqual(decode_published_column(first, "P"), ["beta", "alpha", None])

    def test_columns_must_match_mapping(self):
        publisher = Publisher(self.directory)
        with self.assertRaises(DictionaryError):
            publisher.publish(self.dictionary, {"X": ([0], [1])})
        self.assertIsNone(publisher.current())

    def test_cannot_add_columns_to_completed_version(self):
        publisher = Publisher(self.directory)
        publisher.publish(self.dictionary, {"P": self.columns["P"]})
        with self.assertRaises(DictionaryVersionError):
            publisher.publish(self.dictionary, self.columns)

    def test_published_column_digest_tamper_detected(self):
        publisher = Publisher(self.directory)
        version = publisher.publish(self.dictionary, self.columns)
        csv_path = version.directory / "columns" / "P.csv"
        csv_path.write_text("global_code,validity\n0,1\n", encoding="utf-8")
        with self.assertRaises(DictionaryVersionError):
            read_published_column(version, "P")

    def test_verify_after_publish(self):
        publisher = Publisher(self.directory)
        version = publisher.publish(self.dictionary, self.columns)
        codes, validity = read_published_column(version, "P")
        result = remap_partition(version.dictionary, "P", [0, 1, 0], [1, 1, 0])
        result.global_codes = list(codes)
        report = verify_semantics(
            result,
            version.dictionary,
            original_codes=[0, 1, 0],
            original_validity=[1, 1, 0],
            local_dictionary=["beta", "alpha"],
        )
        self.assertTrue(report.ok, report.errors)

    def test_published_null_never_looks_like_code_zero(self):
        publisher = Publisher(self.directory)
        codes = [0, 0, 1]
        validity = [1, 0, 1]
        document = {
            "partitions": [
                {"key": "P", "dictionary": ["beta", "alpha"], "codes": codes, "validity": validity}
            ]
        }
        dictionary = build_from_partitions_document(document)
        version = publisher.publish(dictionary, {"P": (codes, validity)})
        read_codes, read_validity = read_published_column(version, "P")
        self.assertEqual(read_codes, [1, None, 0])
        self.assertEqual(read_validity, [1, 0, 1])
        # NULL 与“合法值 0（alpha）”必须可区分。
        self.assertEqual(decode_published_column(version, "P"), ["beta", None, "alpha"])


if __name__ == "__main__":
    unittest.main()
