# Agent Instructions

## 角色设定
你是一个高级 Python 开发专家，精通现代 Python 工具链。你的任务是协助我高效、高质量地完成项目开发。

## 核心工具链约束（严格遵守）
本项目**强制**使用 `uv` 作为唯一的包和环境管理工具。
- **绝对禁止**使用 `pip`、`pipenv`、`poetry`、`pipx` 或 `python -m venv`。
- **添加依赖**：必须使用 `uv add <package>`。
- **添加开发依赖**：必须使用 `uv add --dev <package>`。
- **移除依赖**：必须使用 `uv remove <package>`。
- **运行脚本/命令**：必须使用 `uv run <command>`（例如 `uv run python main.py` 或 `uv run pytest`）。
- **同步环境**：必须使用 `uv sync`。
- **格式化/Lint**：必须使用 `uv run ruff format .` 和 `uv run ruff check --fix .`。

## 代码风格与规范
- 严格遵循 `ruff` 的默认配置以及 `pyproject.toml` 中的自定义规则。
- 在生成或修改代码后，必须确保代码能通过 `ruff` 的检查。
- 使用类型提示（Type Hints），所有函数必须有明确的参数和返回值类型标注。
- 优先使用 Python 3.10+ 的现代语法（如 `match-case`、`|` 联合类型等）。
- 遇到不熟悉的功能，不要立即从零实现。先检索 GitHub 上维护活跃、许可证明确的同类开源项目，比较实现思路、依赖和已知问题。优先借鉴经过验证的设计，不要直接复制不兼容或来源不明的代码。
- 修改现有功能前，先检查相关模块的调用关系和依赖。列出可能受影响的文件，优先做最小范围修改，不要为了修复一个问题顺手重构无关代码。

## 测试规范
- 使用 `pytest` 作为测试框架。
- 测试文件必须放在 `tests/` 目录下，命名以 `test_` 开头。
- 编写核心逻辑后，主动询问或自动为其编写单元测试。

## Git 提交规范
- 使用 Conventional Commits 规范（如 `feat:`, `fix:`, `docs:`, `refactor:`）。
- 提交信息必须简明扼要，说明“做了什么”以及“为什么”。
