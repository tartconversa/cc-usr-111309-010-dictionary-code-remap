# 分区字典编码的全局统一与可逆重映射

本项目保存虚构实验设施的仓湖源数据与Python读取工具。全部设备、样品、记录和数值均为合成。

`src/dictionary_reader.py` 只做原始文件读取；`src/dictionary_codec.py` 负责公共字典
构建、列重编码、反向语义核对与版本发布。二者都不修改 fixtures 中的原始资料。

## 字段与单位

每分区dictionary是本地码字典，codes是原始整数列，validity是同长0/1向量；无效位置原code不应被解释成值。公共字典由 `src/dictionary_codec.py` 按需构建，不预置在 fixtures 中。

编码契约（实现见 `src/dictionary_codec.py`）：

* code 是本地数字，不是业务身份；语义值只能由 `dictionary[code]` 得到，禁止直接
  拼接不同分区的编码列。
* NULL 只由 validity 位图表示。`code == -1` 不是 NULL；无效位置的原 code 原样保留
  供审计，但不参与语义值。空字符串 `""` 是合法字典值。
* 本地字典项必须唯一；valid 位置 code 越界即报错。
* 全局字典按 UTF-8 **字节序**排序去重（不是本地化排序或码点/UTF-16 序）。
* 每分区 `local_code -> global_code` 转换表显式存储并绑定 `dictionary_id` 与摘要。
* NFC 合并是另一个编码协议版本（`nfc_utf8_bytes_sorted/v1`），必须换 id/摘要，
  不会修改既有字典；默认 `exact_utf8_bytes_sorted/v1`。

### 版本与追加

追加分区（例如 `aardvark`）会改变全局排序，使既有 global_code 整体平移。因此：
新公共字典必须发布成新版本；所有引用列绑定自己的 `dictionary_id` 与摘要；不得
原地替换字典。旧列用旧版本字典解释。发布是原子的：新版本目录写完 `COMPLETE`
后才切换 `CURRENT`，中断时旧读者不受影响，重试可恢复。

`stable_codes` 提供可选的追加式稳定编码：旧码永不变，但排序语义不再是 UTF-8 全序。
两者不能同时声称。

### 有界内存与字典外排序

* `build_global_dictionary(..., max_buffer_bytes=N)`：外部归并排序后返回内存字典。
* `build_global_dictionary_streaming(source)`：两遍流式构建，不保留大列。
* `external_sort_to_file` / `build_disk_dictionary` / `SortedDictionaryFile`：字典大于
  内存时落盘 + 稀疏索引，`index_of`/`value_of` 随机访问，发布时把字典文件复制进版本。
* `stream_remap` / `stream_write_remap`：大列逐行改码，O(1) 内存；两侧长度不一致会
  报错而不是静默截断。

反向核对 `verify_semantics` 会检查行序、NULL 位置与逻辑值多重集；用错误的
`dictionary_id` 解释旧列会被拒绝。

material-index.json记录全部输入内容哈希；fixtures保存原始资料与配置，计算输出可放.workspace-output，原始资料不应被覆盖。

## 运行

Python 3.11以上，标准库即可，不需安装第三方依赖。

    python -m src.dictionary_reader fixtures/partitions.json
    python -m src.dictionary_codec fixtures/partitions.json --include-append
    python -m src.dictionary_codec fixtures/partitions.json --include-append --nfc
    python -m src.dictionary_codec fixtures/partitions.json --include-append \
        --max-buffer-bytes 4096 --publish .workspace-output/versions \
        --output .workspace-output/report.json
    python -m compileall -q src tests
    python -m unittest discover -s tests -v

`--nfc` 构建 NFC 协议版本；`--max-buffer-bytes` 启用外部归并排序；`--publish` 写入
版本目录（原子发布 + 列映射 + 重编码列）。

## 固定容器

    docker build -t dictionary-code-remap:local .
    docker run --rm dictionary-code-remap:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
