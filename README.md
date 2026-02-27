# 贵金属期货交易终端

每日定时采集贵金属现货及期货行情数据，生成静态 HTML 交易看板。

## 项目结构

```
precious-metals-terminal/
├── config.py           # 品种配置、合约规则、数据源
├── fetcher.py          # 核心数据采集脚本 (每日 9:00 运行)
├── manage.py           # 数据管理工具 (列表/清理/索引)
├── setup.sh            # 一键安装 & 配置定时任务
├── index.html          # 交易终端前端页面
├── README.md           # 本文档
└── data/               # 按日期组织的数据 (自动创建)
    ├── index.json      # 可用日期索引
    ├── 2026-02-26/
    │   └── market_data.json
    ├── 2026-02-27/
    │   └── market_data.json
    └── ...
```

## 快速开始

### 1. 安装依赖

```bash
pip install akshare requests beautifulsoup4 --break-system-packages
```

### 2. 首次采集数据

```bash
python fetcher.py
```

### 3. 打开看板

```bash
# 方法1: 直接用 Python 起一个 HTTP 服务
python -m http.server 8080

# 方法2: 或任意 HTTP 服务器
npx serve .
```

然后浏览器打开 `http://localhost:8080`

> ⚠️ 必须通过 HTTP 服务器访问 (不能直接双击打开 index.html), 因为前端需要通过 fetch 加载 JSON 数据文件。

### 4. 设置每日自动采集

```bash
# 一键设置 (安装依赖 + 首次采集 + 设置cron)
bash setup.sh

# 或手动添加 cron
crontab -e
# 添加: 0 9 * * 1-5 cd /path/to/precious-metals-terminal && python3 fetcher.py >> cron.log 2>&1
```

## 数据源说明

| 模块       | 数据源                       | 接口方式        |
|-----------|------------------------------|----------------|
| 现货K线    | AKShare (新浪期货日线)        | `futures_zh_daily_sina` |
| 实时价格    | AKShare (期货实时行情)        | `futures_zh_spot` |
| 期货合约    | 新浪财经 hq.sinajs.cn + AKShare | HTTP + API |
| 库存数据    | 上期所仓单日报 (AKShare)      | `futures_shfe_warehouse_receipt` |
| 持仓排名    | 上期所 (AKShare)             | `futures_shfe_position_rank` |
| 新闻资讯    | 东方财富搜索 + 新浪财经搜索    | HTTP搜索 |
| 算法预测    | 本地计算 (基差+库存+持仓)      | 内置模型 |

## 支持品种

| 代码 | 品种   | 交易所 | 合约月份          |
|------|--------|--------|-------------------|
| AU   | 黄金   | 上期所 | 2,4,6,8,10,12月   |
| AG   | 白银   | 上期所 | 2,4,6,8,10,12月   |
| CU   | 铜     | 上期所 | 每月              |
| AL   | 铝     | 上期所 | 每月              |

## 数据管理

```bash
# 列出所有可用数据日期
python manage.py list

# 显示今日数据摘要
python manage.py summary

# 显示指定日期摘要
python manage.py summary 2026-02-26

# 生成 data/index.json (前端自动发现可用日期)
python manage.py index

# 清理30天前的旧数据
python manage.py clean

# 清理7天前的旧数据
python manage.py clean 7
```

## 合约自动选取规则

系统自动选取当前日期之后最近的 **3个** 合约:

- 跳过已到期合约 (当月及之前)
- 按品种的合约月份规则生成
- 例: 2026年2月 → `AG2604, AG2606, AG2608`

## 算法预测说明

模型基于以下三个维度分析不同交割率情景:

1. **基差结构**: 期货价 vs 现货价的升贴水关系
2. **库存趋势**: 交易所库存、注册仓单变化方向
3. **持仓分析**: 多空持仓比、持仓集中度

输出 5 个交割率情景 (5%/15%/30%/50%/70%+), 每个情景给出:
- 预估价格
- 发生概率
- 方向判断 (偏多/中性/偏空)
- 情景描述

## 注意事项

- 采集脚本在非交易时间运行时, 获取的是最近一个交易日数据
- 新浪/东方财富接口为免费公开接口, 请控制访问频率
- 五档盘口中, 买2-买5 和 卖2-卖5 为基于买一卖一的模拟数据 (免费接口限制)
- 首次运行可能因接口变动需要微调, 查看 `fetcher.log` 排查
