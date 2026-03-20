Pelegant Data
=============

本仓库用于配合 Codex / GPT 型代理批量抓取和解析各品牌官网的招聘岗位信息，并将结果以结构化 JSON 形式保存，便于后续分析和汇总。

## 仓库结构

- `codex_self_loop.py`：主控制脚本。循环调用 Codex（通过 `codex` 命令行工具），
	读取 `Book2.csv` 中的站点 URL，一行对应一个招聘网站，逐行生成解析器和岗位数据。
- `api_config.py`：保存调用 OpenAI / Codex 所需的 API 配置（如 `API_URL`、`API_KEY`）。
- `Book2.csv`：输入的站点配置表，包含各个目标招聘网站的 URL 以及（可选）期望岗位数量等信息。
- `system_prompt.txt`：传给 Codex 的系统提示词，用于约束解析器的行为和输出契约。
- `artifacts/`
	- `parsers/`：为每个站点自动生成的解析脚本，例如
		`row_01__parser_pgcareers_com_row_1.py`。
	- `jobs/`：对应站点抽取出的岗位 JSON 文件，例如
		`row_01__jobs_pgcareers_com_row_1.json`。
- `result.json`：聚合所有成功行的岗位信息的总汇文件（追加写入）。

## 工作流程概述

1. 你在 `Book2.csv` 中维护一组目标站点及相关元数据（如预期岗位数）。
2. 运行 `codex_self_loop.py`：脚本会按行读取 CSV，为每个站点多轮调用 Codex，
	 要求生成：
	 - 针对该站点的 Python 解析器脚本（保存在 `artifacts/parsers/`）；
	 - 结构化的岗位 JSON（保存在 `artifacts/jobs/`）。
3. 脚本会对生成的文件做基本校验，例如：
	 - 解析器语法检查（`py_compile`）。
	 - 岗位 JSON 是否为非空列表、是否包含 `title` 和 URL 字段、数量是否符合预期等。
4. 校验通过的行，其岗位数据会被追加写入 `result.json` 中，形成全量汇总。

## 环境准备

1. 安装 Python 3.10+。
2. 确保你已经安装并可以在命令行中使用 `codex` CLI 工具。
3. 在 `api_config.py` 中配置正确的 `API_URL` 和 `API_KEY`。
	 - 建议实际使用时不要把真实密钥提交到版本库，可改为从环境变量读取。

## 基本使用

在仓库根目录下：

1. 准备 / 更新 `Book2.csv`，确保包含目标站点信息。
2. 根据需要编辑 `system_prompt.txt`，设定 Codex 的系统提示和约束。
3. 运行主脚本（示例）：

	 ```bash
	 python codex_self_loop.py \
		 --system-prompt-file system_prompt.txt \
		 --csv-file Book2.csv \
		 --iterations 100 \
		 --aggregate-file result.json
	 ```

	 常用可选参数（根据脚本内置说明）：

	 - `--start-index`：从 CSV 的第几条记录（1-based）开始处理。
	 - `--site-column` / `--count-column`：显式指定 URL / 预期岗位数字段名（否则自动推断）。
	 - `--max-attempts-per-target`：单个站点的重试次数。
	 - `--dry-run`：只生成提示词和元数据，不实际调用 Codex。

运行完成后，你可以在：

- `artifacts/parsers/` 查看为各站点生成的解析器源码；
- `artifacts/jobs/` 查看对应的岗位列表；
- `result.json` 中查看聚合后的所有岗位数据。

## 注意事项

- 本仓库中的 `api_config.py` 目前以明文方式存放 API Key，仅供本地测试之用。
	在真实项目中应改为通过环境变量或密钥管理服务加载，避免泄露。
- 生成的解析器脚本与岗位 JSON 文件以行号和域名区分，
	请勿手动重命名，否则可能影响后续运行时的校验和聚合逻辑。
- 如果脚本运行时频繁校验失败，可检查：
	- 目标站点页面结构是否发生较大变化；
	- `system_prompt.txt` 是否给予了足够清晰的抽取规则；
	- `Book2.csv` 中的预期岗位数量是否合理。

## 许可

当前仓库未显式声明开源许可证，如需在个人项目以外的场景使用或分发，
请先与仓库所有者沟通确认。

