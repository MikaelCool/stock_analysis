# Daily Stock Analysis

面向 A 股实盘辅助的智能分析与半自动化选股系统。项目整合了大模型、量化规则、市场情绪、新闻检索、自动回测与飞书通知，提供统一的 Web 工作台。

## 项目特点

- 面向实盘流程：从收盘后扫描、次日操作手册，到 T+1/T+3/T+5/T+10 回测与参数优化，链路完整。
- 多数据源回退：TickFlow 优先，Tushare 辅助；新闻搜索支持 Tavily、Bocha 等多源回退。
- 大模型与量化结合：不仅生成分析文本，还融合技术形态、量价结构、市场情绪与消息过滤。
- Web 优先：问股、选股、大盘情绪、运行记录、回测总结都可直接在网页使用。
- 可自动化运行：支持定时任务、同日缓存、本地数据库、飞书推送。

## 核心功能

### 1. 问股分析

- 输入股票代码或名称，生成多维度分析结论
- 支持技术面、消息面、板块联动、市场环境综合判断
- 支持 OpenAI 兼容模型、DeepSeek、Ollama 等

### 2. 收盘后选股

- 固定范围：沪深主板、非 ST
- 支持多种内置策略与自定义 YAML 策略
- 每个策略默认最多输出 10 只候选股
- 只对前 5 只生成详细操作手册

### 3. 次日操作手册

- 输出高开、平开、小低开三种执行预案
- 给出观察位、介入条件、止损线、放弃条件
- 适合 5 到 10 天波段跟踪

### 4. 回测与优化

- 自动回填 T+1、T+3、T+5、T+10 表现
- 生成优化总结而不是只展示原始配置
- 支持同日缓存与历史结果落库

### 5. 情绪与通知

- 提供沪 A 大盘情绪指标
- 支持飞书机器人通知
- 支持定时在每个交易日 16:00 后自动执行选股

## 技术栈

- 前端：React + Vite
- 后端：FastAPI
- 数据库：SQLite
- 模型路由：LiteLLM
- 行情数据：TickFlow、Tushare
- 新闻搜索：Tavily、Bocha

## 快速开始

### Windows 本地部署

```powershell
git clone https://github.com/MikaelCool/stock_analysis.git
cd stock_analysis
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
cd apps\dsa-web
npm install
cd ..\..
copy .env.example .env
```

填写 `.env` 后启动：

```powershell
.\.venv\Scripts\python.exe main.py --webui-only --host 0.0.0.0 --port 8000
```

访问：

- 首页：`http://127.0.0.1:8000`
- 问股页：`http://127.0.0.1:8000/chat`
- 选股页：`http://127.0.0.1:8000/picker`
- 接口文档：`http://127.0.0.1:8000/docs`

## 文档

- 中文部署文档：[docs/DEPLOY_ZH.md](docs/DEPLOY_ZH.md)
- 首版发布说明：[docs/RELEASE_v1.0.0.md](docs/RELEASE_v1.0.0.md)
- 详细配置模板：[.env.example](.env.example)

## 安全说明

- 不要把 `.env`、数据库、日志、缓存目录提交到公开仓库
- 如果你需要公开演示，请只提交 `.env.example`
- 飞书 Webhook、模型 API Key、TickFlow/Tushare/Search Key 都必须保留在本地

## License

MIT
