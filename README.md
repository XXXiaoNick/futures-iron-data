# 贵金属期货交易终端

离线优先的贵金属期货数据终端，一键采集 → 生成静态 HTML，双击即可打开，无需服务器。

## 核心功能

- **实时行情** — K线图 + 合约报价 + 持仓量/成交量
- **AI 综合研判** — IDAF 三因子情景模型（DSCR × Basis-Carry × 技术面），输出 T+1/T+5 多空方向与置信度
- **逐合约交割危机** — 五因子 Logistic 模型，对近3个合约分别计算交割压力概率（Tab 切换展示）
- **多源新闻聚合** — 12个新闻源 + FinBERT/LM 双算法情绪分析
- **CME/CFTC 深度数据** — 8个官方数据源多层 Fallback

## 支持品种

| 品种 | 代码 | 交易所 | 合约乘数 |
|------|------|--------|----------|
| 黄金 | AU | SHFE | 1000g/手 |
| 白银 | AG | SHFE | 15kg/手 |
| 铜 | CU | SHFE | 5吨/手 |
| 铝 | AL | SHFE | 5吨/手 |
| 铂金 | PT | SGE | 1g/手 |
| 钯金 | PD | NYMEX | 100oz/手 |

## 项目结构

```
precious-metals-terminal/
├── fetcher.py          # 数据采集 + 模型计算 (5600行)
├── build.py            # HTML 生成器 (870行)
├── config.py           # 品种配置 + 合约规则
├── data/
│   └── YYYY-MM-DD/
│       └── market_data.json
├── docs/
│   └── index.html      # 最终输出页面
└── README.md
```

## 快速开始

### 安装依赖

```bash
pip install akshare yfinance requests pandas --break-system-packages
```

### 运行

```bash
# 1. 采集数据 (自动保存到 data/当日日期/)
python fetcher.py

# 2. 生成页面
python build.py

# 3. 打开
open docs/index.html     # macOS
# 或 xdg-open docs/index.html  # Linux
# 或直接双击 docs/index.html
```

重建历史日期的页面：

```bash
python build.py 2026-02-26
```

## 数据架构

### 采集管线 (`fetcher.py`)

```
fetch_all()
  ├── fetch_realtime_price()      # 新浪期货实时报价
  ├── fetch_contract_quotes()     # AKShare 合约明细 (OI/Vol/结算价)
  ├── fetch_spot_kline()          # 60日K线 (新浪/AKShare)
  ├── fetch_cme_stocks()          # CME仓库库存 (.xls解析)
  ├── fetch_cme_crisis_data()     # CME/CFTC 8大数据源汇总
  ├── fetch_indicators()          # SHFE库存 + 持仓 + 技术指标
  ├── fetch_news()                # 12源新闻 + 情绪分析
  ├── compute_predictions()       # IDAF三因子研判模型
  ├── compute_delivery_crisis()   # 五因子综合危机概率
  └── compute_per_contract_crisis()  # 逐合约危机 (近3月)
```

### CME/CFTC 数据源

| 因子 | 数据 | 源优先级 |
|------|------|----------|
| F1 覆盖率 | 前月OI | CME Bulletin PDF → Quotes API → yfinance → AKShare |
| F2 期限结构 | 结算价 | CME Settlements HTML → API → Quotes API |
| F3 库存流动 | 交割通知 | CME YTD PDF → 入口页PDF链接 |
| F3 库存流动 | 日变化 | CME Stocks .xls → 本地CSV历史 |
| F4 时点 | 日历 | CME Calendar HTML → 推算 |
| F5 压力 | 保证金 | CME Margins HTML → API + Clearing Notices |
| F5 压力 | COT | CFTC 短格式 → 长格式 → CSV |
| F5 压力 | 升水 | Stooq → yfinance → 新浪外汇 |

### 新闻源

**国内**: 东方财富(2个)、新浪财经、财联社、雪球、金十数据、同花顺

**国际**: Google RSS、Reuters、Bloomberg、Kitco、Investing.com

**情绪算法**: FinBERT 词典 + Loughran-McDonald 金融词典 → 加权融合

## 模型说明

### IDAF 三因子研判

综合交割供需比(DSCR)、基差-持有成本偏离(Basis-Carry)、技术面动量，通过五情景蒙特卡洛模拟输出：

- 最可能交割率及对应价格
- 概率加权预期价格
- T+1 / T+5 方向判断 + 置信度

### 五因子交割危机模型

```
P(crisis) = Logistic( w₁·F1 + w₂·F2 + w₃·F3 + w₄·F4 + w₅·F5 )

F1 覆盖率 (30%)  — 预期交割需求 / 可交割供给
F2 期限结构 (25%) — Backwardation程度 (近月vs远月价差)
F3 库存流动 (20%) — 注册仓单变化 + E/R缓冲 + 连续流失天数
F4 时点 (10%)     — 距FND天数 (指数衰减)
F5 压力 (15%)     — 年化波动率 + 保证金 + COT集中度 + 量比 + LBMA升水
```

逐合约模式下，F1/F2/F4 使用各合约自身数据，F3/F5 品种级共享。

输出范围 1%~95%，等级：低(<15%) → 中低(15-30%) → 中等(30-50%) → 高(50-70%) → 极高(>70%)

## 前端功能

- 品种 Tab 切换 (AU/AG/CU/AL/PT/PD)
- 响应式双栏布局 (左: K线+合约 | 右: 研判+危机+新闻)
- 逐合约交割危机 Tab 切换 (如 AU2604 / AU2606 / AU2608)
- 非交易日容错 + Google Fonts 国内镜像 + 非阻塞加载

## 开发说明

`build.py` 在生成页面时会自动补算缺失数据（情绪分析、IDAF预测、交割危机、逐合约危机），兼容历史数据重建。

所有数据序列化为 JSON 内嵌到 `index.html`，纯静态页面，可直接部署到 GitHub Pages 等静态托管。
