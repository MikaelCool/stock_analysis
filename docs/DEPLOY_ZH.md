# 中文部署文档

本文档面向公开仓库使用者，说明如何在本地或服务器部署并运行 `Daily Stock Analysis`。

## 1. 部署前准备

建议环境：

- Windows 10/11 或 Linux
- Python 3.10 及以上
- Node.js 18 及以上
- Git

可选但推荐：

- 一个 OpenAI 兼容模型网关，或 DeepSeek API
- TickFlow API Key
- Tushare Token 或兼容网关
- Tavily 与 Bocha 中至少一个新闻搜索 Key
- 飞书机器人 Webhook

## 2. 目录说明

核心目录：

- `apps/dsa-web/`：前端页面
- `api/`：FastAPI 接口
- `src/`：核心业务逻辑
- `strategies/`：选股策略 YAML
- `docs/`：说明文档

运行中会生成但不应提交的目录：

- `data/`
- `logs/`
- `reports/`

## 3. Windows 本地部署

### 3.1 克隆项目

```powershell
git clone https://github.com/MikaelCool/stock_analysis.git
cd stock_analysis
```

### 3.2 创建虚拟环境并安装后端依赖

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```

### 3.3 安装前端依赖

```powershell
cd apps\dsa-web
npm install
cd ..\..
```

### 3.4 配置环境变量

```powershell
copy .env.example .env
```

至少建议填写这些项目：

- `LLM_CHATGPT54_BASE_URL`
- `LLM_CHATGPT54_API_KEY`
- `LLM_DEEPSEEK_API_KEY`
- `TICKFLOW_API_KEY`
- `TUSHARE_TOKEN`
- `TAVILY_API_KEYS`
- `BOCHA_API_KEYS`
- `FEISHU_WEBHOOK_URL`
- `FEISHU_WEBHOOK_SECRET`

如果你使用 Tushare 兼容网关，可以填写：

```env
TUSHARE_API_URL=http://your-tushare-compatible-gateway
```

## 4. 启动项目

### 4.1 前台启动

```powershell
.\.venv\Scripts\python.exe main.py --webui-only --host 0.0.0.0 --port 8000
```

### 4.2 PowerShell 脚本启动

前台脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_web.ps1
```

后台脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\start_web_bg.ps1
```

## 5. Web 入口

启动成功后访问：

- 首页：`http://127.0.0.1:8000`
- 问股页：`http://127.0.0.1:8000/chat`
- 选股页：`http://127.0.0.1:8000/picker`
- 健康检查：`http://127.0.0.1:8000/api/health`
- API 文档：`http://127.0.0.1:8000/docs`

## 6. 选股功能说明

选股页提供：

- 策略下拉框
- 当日扫描结果
- Runs 历史记录
- 指标快照表格
- 回测优化总结

默认规则：

- 股票池：沪深主板、非 ST
- 每种策略最多保留 10 只候选股
- 前 5 只生成详细操作手册

## 7. 定时任务

项目支持每日交易日 16:00 后自动执行默认策略。

示例配置：

```env
STOCK_PICKER_SCHEDULE_ENABLED=true
STOCK_PICKER_SCHEDULE_CRON=0 0 16 * * 1-5
STOCK_PICKER_DEFAULT_STRATEGY=mainboard_swing_master
```

默认行为：

- 16:00 后自动执行“主力波段双模”
- 写入扫描记录
- 后台补新闻与大模型操作手册
- 将前 5 只的操作手册推送到飞书

## 8. 飞书通知配置

示例：

```env
FEISHU_ENABLED=true
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/your-hook-id
FEISHU_WEBHOOK_SECRET=your-secret
```

建议先在网页中手动执行一次扫描，确认飞书消息链路可用。

## 9. 数据源建议

推荐组合：

- 历史日线与实时行情：`TickFlow`
- 补充与回退：`Tushare`
- 新闻搜索：`Tavily + Bocha`
- 大模型：`ChatGPT 5.4 + DeepSeek`

当前项目已经支持：

- 同日数据缓存
- 新闻按股票落库缓存
- 低相关搜索结果继续回退
- 美股情绪双源回退

## 10. Docker 部署

如果你更适合容器部署，可参考仓库中的：

- `docker/`
- `docs/deploy-webui-cloud.md`
- `docs/DEPLOY_EN.md`

## 11. 常见问题

### 11.1 页面打开很慢

优先检查：

- `.env` 是否完整
- `TickFlow` 与 `Tushare` 是否至少一个可用
- 新闻搜索 Key 是否配置
- 模型网关是否可访问

### 11.2 选股没有结果

常见原因：

- 当前策略阈值偏严
- 历史数据窗口不足
- 上游数据源未补全
- 大盘情绪过滤较强

建议先切换策略，再看扫描结果与回测总结。

### 11.3 中文乱码

Windows 下建议使用项目自带脚本启动，或确保终端编码为 UTF-8。

## 12. 安全建议

- `.env` 只保留在本地
- 不要把真实 API Key 写进 `README`、截图或 issue
- 对外公开仓库时，仅提交 `.env.example`
