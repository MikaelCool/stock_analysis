# Daily Stock Analysis

基于大模型的股票分析与半自动化交易辅助系统，支持 A 股、港股、美股的问股分析、收盘后选股、消息检索、回测验证、飞书等多渠道通知，以及 Web 工作台使用。

## 项目优势

1. 大模型与量化规则融合  
   不是只输出一段 AI 文本，而是把技术面、量价结构、消息面、市场情绪、回测结果整合到同一条分析链路里。

2. 支持半自动化波段交易流程  
   从收盘后扫描候选股，到次日操作手册，到 T+1/T+3/T+5/T+10 回测，再到参数优化，链路完整。

3. 多数据源回退，稳定性更高  
   行情、指数、新闻都不是单点依赖。一个源失败会自动切下一个，降低接口波动影响。

4. Web、Bot、API 共用同一套能力  
   可以在网页里问股、选股，也可以继续走接口或消息通知渠道，不需要维护多套逻辑。

5. 面向实盘使用而不是演示  
   支持定时任务、本地缓存、历史入库、飞书推送、后台异步富化、全市场扫描，适合长期运行。

## 核心功能

### 1. 问股分析
- 输入股票代码或名称，生成多维度分析结果
- 结合技术面、消息面、市场环境给出结论
- 支持 ChatGPT 5.4、DeepSeek 等 OpenAI 兼容模型

### 2. 收盘后选股
- 支持沪深主板非 ST 全市场扫描
- 内置多种策略：均线金叉、缠论、波浪理论、多头趋势等
- 支持自定义策略 YAML
- 每次扫描最多保留 10 只候选，仅对前 5 只生成详细操作手册

### 3. 次日操作手册
- 根据量化结果和消息面，为次日生成平开、高开、低开的执行预案
- 给出止损、止盈、放弃条件、5 到 10 天跟踪重点

### 4. 回测与优化
- 自动回填 T+1、T+3、T+5、T+10 结果
- 根据历史样本生成优化总结
- 支持将优化结果回灌到选股参数

### 5. 大盘情绪与消息检索
- A 股大盘情绪分数
- 美股情绪代理指标
- 新闻搜索支持多源回退与低相关结果过滤
- 同日新闻结果按股票落库缓存

### 6. 通知与自动化
- 飞书、企业微信、Telegram、Discord、Slack、邮件等通知
- 支持定时运行
- 可在每天收盘后自动执行主力波段双模策略并推送飞书

## 系统架构概览

- 前端：React + Vite
- 后端：FastAPI
- 数据库：SQLite
- 大模型：LiteLLM 路由 OpenAI 兼容模型
- 行情数据：TickFlow、Tushare，以及其他回退源
- 新闻数据：Tavily、Bocha、SearXNG 等

## 部署方式

### 方式一：Windows 本地部署

适合日常桌面使用。

#### 1. 准备环境

- Python 3.10+
- Node.js 18+
- Git

#### 2. 克隆项目

```powershell
git clone https://github.com/ZhuLinsen/daily_stock_analysis.git
cd daily_stock_analysis
```

#### 3. 创建虚拟环境并安装依赖

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```

#### 4. 安装前端依赖

```powershell
cd apps\dsa-web
npm install
cd ..\..
```

#### 5. 配置 `.env`

建议复制一份模板后填写：

```powershell
copy .env.example .env
```

至少需要配置这些项目：

```env
WEBUI_ENABLED=true
WEBUI_HOST=0.0.0.0
WEBUI_PORT=8000

LLM_CHANNELS=deepseek,chatgpt54
LITELLM_MODEL=openai/gpt-5.4
AGENT_LITELLM_MODEL=openai/gpt-5.4
LITELLM_FALLBACK_MODELS=deepseek/deepseek-chat,deepseek/deepseek-reasoner

LLM_CHATGPT54_PROTOCOL=openai
LLM_CHATGPT54_BASE_URL=http://your-openai-compatible-gateway/v1
LLM_CHATGPT54_API_KEY=your_key
LLM_CHATGPT54_MODELS=gpt-5.4

LLM_DEEPSEEK_PROTOCOL=deepseek
LLM_DEEPSEEK_BASE_URL=https://api.deepseek.com/v1
LLM_DEEPSEEK_API_KEY=your_key
LLM_DEEPSEEK_MODELS=deepseek-chat,deepseek-reasoner

TUSHARE_TOKEN=your_token
TUSHARE_API_URL=http://your-tushare-compatible-gateway
TICKFLOW_API_KEY=your_key

TAVILY_API_KEYS=your_key
BOCHA_API_KEYS=your_key

FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxxx
FEISHU_WEBHOOK_SECRET=your_secret

STOCK_PICKER_ENABLED=true
STOCK_PICKER_SCHEDULE_ENABLED=true
```

如果你使用的是本项目当前推荐的收盘后选股模式，还建议增加：

```env
STOCK_PICKER_DEFAULT_STRATEGY=mainboard_swing_master
STOCK_PICKER_MAX_CANDIDATES=10
STOCK_PICKER_LLM_REVIEW_LIMIT=5
```

### 方式二：Docker 部署

适合服务器常驻运行。

```bash
git clone https://github.com/ZhuLinsen/daily_stock_analysis.git
cd daily_stock_analysis
cp .env.example .env
# 编辑 .env
docker compose up -d --build
```

如果你只需要 Web 服务，也可以直接运行项目文档中的单服务部署方式。

## 启动项目

### Windows 前台启动

```powershell
cd D:\codex\daily_stock_analysis
.\.venv\Scripts\python.exe main.py --webui-only --host 0.0.0.0 --port 8000
```

### Windows 后台启动

```powershell
powershell -ExecutionPolicy Bypass -File D:\codex\daily_stock_analysis\start_web_bg.ps1
```

### 定时任务模式启动

如果你要让系统长期运行并使用内置调度器：

```powershell
.\.venv\Scripts\python.exe main.py --schedule --serve
```

## 如何进入网页

启动成功后，浏览器打开：

- 首页：`http://127.0.0.1:8000`
- 问股页：`http://127.0.0.1:8000/chat`
- 选股页：`http://127.0.0.1:8000/picker`
- 接口文档：`http://127.0.0.1:8000/docs`
- 健康检查：`http://127.0.0.1:8000/api/health`

## 如何使用

### 1. 问股

1. 打开 `/chat`
2. 输入股票代码或名称
3. 查看模型给出的分析结论、消息面和操作建议

### 2. 手动选股

1. 打开 `/picker`
2. 在左侧选择策略
3. 调整策略参数
4. 点击“立即扫描并推送飞书”
5. 页面会先返回候选股，再后台补充新闻、操作手册、回测和优化结果

### 3. 查看候选股

- 每次最多显示 10 只候选
- 前 5 只会生成详细操作手册
- 可以查看每只股票的：
  - 量化结论
  - 止损止盈
  - 次日操作手册
  - 指标快照
  - T+1/T+3/T+5/T+10 回测结果

### 4. 自动收盘后选股

如果启用了后台调度：

- 每天 `16:00` 自动运行 `主力波段双模`
- 自动扫描沪深主板非 ST
- 自动生成前 5 只操作手册
- 自动推送飞书

## 数据源策略

### 行情数据

- A 股历史与实时：优先 TickFlow，Tushare 辅助回退
- 全市场选股日线：优先 TickFlow 批量接口
- 大盘与指数：多源回退

### 新闻数据

- Bocha
- Tavily
- SearXNG 等后备源

规则：

- 若当前搜索源结果相关性低，会继续回退
- 同日已查过的股票新闻直接从本地数据库读取，减少接口消耗

## 目录说明

```text
daily_stock_analysis/
├─ api/                    # FastAPI 接口
├─ apps/dsa-web/           # Web 前端
├─ data_provider/          # 行情与指数数据源
├─ src/services/           # 业务服务，包括选股、回测、搜索等
├─ strategies/             # 内置与自定义策略
├─ main.py                 # 主入口
├─ start_web.ps1           # Windows 前台启动脚本
└─ start_web_bg.ps1        # Windows 后台启动脚本
```

## 常见问题

### 1. 网页打不开

先检查：

```powershell
curl http://127.0.0.1:8000/api/health
```

返回 `200` 说明服务正常。

### 2. PowerShell 提示脚本无法执行

使用：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_web_bg.ps1
```

### 3. 选股很慢

第一次全市场扫描需要拉取和整理历史数据。后续会复用本地缓存，并把新闻和 LLM 放到后台处理，速度会明显快很多。

### 4. 为什么没有选出股票

可能原因：

- 当天策略阈值偏严
- 当天市场结构不匹配该策略
- 历史样本不足导致优化参数未形成有效结论

你可以在 `/picker` 页面调整参数后再扫描。

## 免责声明

本项目仅用于研究、学习和交易辅助，不构成任何投资建议。实盘交易请自行承担风险。
