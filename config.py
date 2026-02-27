"""
贵金属期货交易终端 - 配置文件
===========================
定义品种、合约规则、数据源等核心配置
"""

import os
from datetime import datetime

# ─── 路径配置 ───
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# ─── 品种配置 ───
# exchange: SHFE=上期所, DCE=大商所, CZCE=郑商所
#
# source_map: 各数据源的品种代码映射 (统一管理, 避免 PD/PA 混用)
#   cme_product  — CME 产品代码 (GC/SI/HG/PL/PA)
#   yahoo        — Yahoo Finance 期货合约 (GC=F / PA=F ...)
#   stooq_spot   — Stooq 现货报价 (xauusd / xpdusd ...), 仅作参考价
#   sina_foreign — 新浪外盘期货行情代码 (hf_AU / hf_PA ...)
#   akshare_hist — AKShare futures_foreign_hist 参数
#
# data_requirements: 该品种必须具备的数据类型
#   "hist"      — 历史K线
#   "realtime"  — 实时价格
#   "inventory" — 库存数据
#   "oi"        — 持仓量

METALS = {
    "AU": {
        "name": "黄金",
        "name_en": "Gold",
        "exchange": "SHFE",
        "unit": "元/克",
        "contract_unit": "1000克/手",
        "contract_size": 1000,
        "delivery_multiplier": 1,
        "tick_size": 0.02,
        "color": "#F5A623",
        "akshare_symbol": "au",
        "sina_spot_symbol": "hf_AU",
        "inventory_symbol": "沪金",
        "contract_months": [2, 4, 6, 8, 10, 12],
        "storage_cost_daily": 0.25,
        "financing_rate_annual": 0.022,
        "delivery_fee": 0.06,
        "historical_delivery_rate": 0.28,
        "keywords_en": ["gold price", "gold futures", "XAUUSD", "COMEX gold",
                        "gold ETF", "central bank gold", "gold demand"],
        "source_map": {
            "cme_product": "GC",
            "yahoo": "GC=F",
            "stooq_spot": "xauusd",
            "sina_foreign": "hf_AU",
            "akshare_hist": "AU",
        },
        "data_requirements": ["hist", "realtime", "inventory", "oi"],
    },
    "AG": {
        "name": "白银",
        "name_en": "Silver",
        "exchange": "SHFE",
        "unit": "元/千克",
        "contract_unit": "15千克/手",
        "contract_size": 15,
        "delivery_multiplier": 15,
        "tick_size": 1,
        "color": "#C0C0C0",
        "akshare_symbol": "ag",
        "sina_spot_symbol": "hf_AG",
        "inventory_symbol": "沪银",
        "contract_months": [2, 4, 6, 8, 10, 12],
        "storage_cost_daily": 0.50,
        "financing_rate_annual": 0.022,
        "delivery_fee": 1.0,
        "historical_delivery_rate": 0.25,
        "keywords_en": ["silver price", "silver futures", "XAGUSD", "COMEX silver",
                        "silver ETF", "silver demand", "silver supply"],
        "source_map": {
            "cme_product": "SI",
            "yahoo": "SI=F",
            "stooq_spot": "xagusd",
            "sina_foreign": "hf_AG",
            "akshare_hist": "AG",
        },
        "data_requirements": ["hist", "realtime", "inventory", "oi"],
    },
    "CU": {
        "name": "铜",
        "name_en": "Copper",
        "exchange": "SHFE",
        "unit": "元/吨",
        "contract_unit": "5吨/手",
        "contract_size": 5,
        "delivery_multiplier": 5,
        "tick_size": 10,
        "color": "#B87333",
        "akshare_symbol": "cu",
        "sina_spot_symbol": "hf_CU",
        "inventory_symbol": "沪铜",
        "contract_months": list(range(1, 13)),
        "storage_cost_daily": 0.40,
        "financing_rate_annual": 0.022,
        "delivery_fee": 5.0,
        "historical_delivery_rate": 0.22,
        "keywords_en": ["copper price", "copper futures", "LME copper",
                        "COMEX copper", "copper demand", "copper supply"],
        "source_map": {
            "cme_product": "HG",
            "yahoo": "HG=F",
            "stooq_spot": None,
            "sina_foreign": "hf_CU",
            "akshare_hist": "CU",
        },
        "data_requirements": ["hist", "realtime", "inventory", "oi"],
    },
    "AL": {
        "name": "铝",
        "name_en": "Aluminum",
        "exchange": "SHFE",
        "unit": "元/吨",
        "contract_unit": "5吨/手",
        "contract_size": 5,
        "delivery_multiplier": 5,
        "tick_size": 5,
        "color": "#848789",
        "akshare_symbol": "al",
        "sina_spot_symbol": "hf_AL",
        "inventory_symbol": "沪铝",
        "contract_months": list(range(1, 13)),
        "storage_cost_daily": 0.30,
        "financing_rate_annual": 0.022,
        "delivery_fee": 5.0,
        "historical_delivery_rate": 0.20,
        "keywords_en": ["aluminum price", "aluminium futures", "LME aluminum",
                        "aluminum demand", "aluminum supply"],
        "source_map": {
            "cme_product": "AL",
            "yahoo": None,
            "stooq_spot": None,
            "sina_foreign": "hf_AL",
            "akshare_hist": "AL",
        },
        "data_requirements": ["hist", "realtime", "inventory", "oi"],
    },
    "PT": {
        "name": "铂金",
        "name_en": "Platinum",
        "exchange": "SGE",
        "unit": "元/克",
        "contract_unit": "1克",
        "contract_size": 1,
        "tick_size": 0.01,
        "color": "#E5E4E2",
        "akshare_symbol": "Pt99.95",
        "sina_spot_symbol": "hf_PL",
        "foreign_symbol": "PL",
        "inventory_symbol": "",
        "contract_months": [],
        "is_spot": True,
        "storage_cost_daily": 0,
        "financing_rate_annual": 0.045,
        "delivery_fee": 0,
        "historical_delivery_rate": 0,
        "keywords_en": ["platinum price", "platinum futures", "NYMEX platinum",
                        "platinum demand", "platinum supply", "XPTUSD"],
        "source_map": {
            "cme_product": "PL",        # CME 铂金产品代码
            "yahoo": "PL=F",            # Yahoo 铂金期货
            "stooq_spot": "xptusd",     # Stooq 现货参考 (非期货!)
            "sina_foreign": "hf_PL",
            "akshare_hist": "PL",       # AKShare 外盘代码
        },
        "data_requirements": ["hist", "realtime"],  # 库存走 PA-PL 合并报表
    },
    "PD": {
        "name": "钯金",
        "name_en": "Palladium",
        "exchange": "NYMEX",
        "unit": "美元/盎司",
        "contract_unit": "100盎司",
        "contract_size": 100,
        "tick_size": 0.5,
        "color": "#CED0CE",
        "akshare_symbol": "",
        "sina_spot_symbol": "hf_PA",    # 新浪用 PA
        "foreign_symbol": "PA",
        "inventory_symbol": "",
        "contract_months": [],
        "is_spot": True,
        "storage_cost_daily": 0,
        "financing_rate_annual": 0.045,
        "delivery_fee": 0,
        "historical_delivery_rate": 0,
        "keywords_en": ["palladium price", "palladium futures", "NYMEX palladium",
                        "palladium demand", "palladium supply", "XPDUSD"],
        "source_map": {
            "cme_product": "PA",        # CME 钯金产品代码 (注意: 不是 PD!)
            "yahoo": "PA=F",            # Yahoo 钯金期货
            "stooq_spot": "xpdusd",     # Stooq 现货参考 (非期货!)
            "sina_foreign": "hf_PA",
            "akshare_hist": "PA",       # AKShare 外盘代码
        },
        "data_requirements": ["hist", "realtime"],  # 库存走 PA-PL 合并报表
    },
}

# ─── 新闻源配置 ───
NEWS_SOURCES = {
    "sina_finance": {
        "name": "新浪财经",
        "base_url": "https://finance.sina.com.cn",
        "search_api": "https://search.sina.com.cn/news",
    },
    "eastmoney": {
        "name": "东方财富",
        "base_url": "https://futures.eastmoney.com",
        "news_api": "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns",
    },
}

# ─── 数据获取超时 ───
REQUEST_TIMEOUT = 15  # 秒
MAX_RETRIES = 3

# ─── 日志配置 ───
LOG_FILE = os.path.join(BASE_DIR, "fetcher.log")


def get_today_dir():
    """获取今日数据目录"""
    today = datetime.now().strftime("%Y-%m-%d")
    d = os.path.join(DATA_DIR, today)
    os.makedirs(d, exist_ok=True)
    return d


def get_nearest_contracts(metal_id: str, n: int = 3) -> list:
    """
    根据当前日期计算最近的 n 个合约代码
    例如: 2026年2月 -> AG2604, AG2606, AG2608
    规则: 跳过已经到期或即将到期(当月)的合约
    """
    metal = METALS[metal_id]
    if metal.get("is_spot"):
        return []  # 现货品种无期货合约
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    months = metal["contract_months"]
    prefix = metal_id.upper()

    contracts = []
    year = current_year
    while len(contracts) < n:
        for m in months:
            if year == current_year and m <= current_month:
                continue
            code = f"{prefix}{str(year)[2:]}{m:02d}"
            contracts.append(code)
            if len(contracts) >= n:
                break
        year += 1

    return contracts