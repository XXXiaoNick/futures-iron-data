#!/usr/bin/env python3
"""
生成模拟数据用于测试前端展示
在无法访问真实 API 时使用: python generate_demo.py
"""

import json
import os
import random
import math
from datetime import datetime, timedelta
from config import METALS, get_today_dir, get_nearest_contracts

random.seed(42)

def gen_kline(metal, days=120):
    data = []
    price = {
        "AU": 685.5, "AG": 8245, "CU": 78650, "AL": 20450
    }.get(metal["akshare_symbol"].upper(), 1000)
    vol_base = {"AU": 2.8, "AG": 85, "CU": 350, "AL": 120}.get(metal["akshare_symbol"].upper(), 10)
    now = datetime.now()
    for i in range(days, 0, -1):
        dt = now - timedelta(days=i)
        if dt.weekday() >= 5:
            continue
        change = (random.random() - 0.48) * vol_base
        o = round(price, 2)
        price += change
        c = round(price, 2)
        h = round(max(o, c) + random.random() * vol_base * 0.5, 2)
        l = round(min(o, c) - random.random() * vol_base * 0.5, 2)
        v = random.randint(50000, 250000)
        data.append({"date": dt.strftime("%Y-%m-%d"), "open": o, "high": h, "low": l, "close": c, "volume": v})
    return data


NEWS_POOL = {
    "AU": [
        {"title": "美联储会议纪要释放鸽派信号，金价冲击历史新高", "source": "路透社", "impact": "positive"},
        {"title": "全球央行黄金储备连续第18个月净增加", "source": "世界黄金协会", "impact": "positive"},
        {"title": "中东地缘政治紧张局势升级推动避险需求", "source": "彭博社", "impact": "positive"},
        {"title": "COMEX黄金库存降至三个月新低", "source": "CME Group", "impact": "neutral"},
        {"title": "美元指数走弱至103关口下方", "source": "新华财经", "impact": "positive"},
        {"title": "上海黄金交易所溢价持续扩大反映实物需求强劲", "source": "上金所", "impact": "positive"},
        {"title": "黄金ETF持仓量连续五周增长", "source": "彭博社", "impact": "positive"},
        {"title": "全球经济不确定性推升贵金属避险配置需求", "source": "高盛", "impact": "positive"},
    ],
    "AG": [
        {"title": "光伏产业链扩产带动白银工业需求大幅增长", "source": "SMM", "impact": "positive"},
        {"title": "白银ETF持仓量突破年内新高", "source": "彭博社", "impact": "positive"},
        {"title": "COMEX白银期货未平仓合约增长显著", "source": "CME Group", "impact": "neutral"},
        {"title": "墨西哥白银矿产量因环保政策收紧下降", "source": "路透社", "impact": "positive"},
        {"title": "电子废料回收白银量同比增长12%", "source": "GFMS", "impact": "negative"},
        {"title": "印度白银进口关税调整方案进入讨论阶段", "source": "经济时报", "impact": "neutral"},
    ],
    "CU": [
        {"title": "智利Codelco铜矿因罢工停产影响全球供应", "source": "路透社", "impact": "positive"},
        {"title": "中国铜冶炼加工费(TC/RC)跌至历史低位", "source": "SMM", "impact": "positive"},
        {"title": "新能源汽车产业链推动铜消费持续增长", "source": "新华财经", "impact": "positive"},
        {"title": "LME铜库存连续第五周下降", "source": "LME", "impact": "positive"},
        {"title": "全球铜矿品位持续下降引发长期供应担忧", "source": "Wood Mackenzie", "impact": "positive"},
        {"title": "国内精炼铜产量保持高位运行态势", "source": "国家统计局", "impact": "negative"},
    ],
    "AL": [
        {"title": "云南电解铝复产进度不及预期", "source": "SMM", "impact": "positive"},
        {"title": "欧洲能源危机持续影响铝冶炼成本", "source": "路透社", "impact": "positive"},
        {"title": "建筑用铝需求回暖带动下游开工率提升", "source": "新华财经", "impact": "positive"},
        {"title": "俄罗斯铝出口限制措施延长至年底", "source": "彭博社", "impact": "positive"},
        {"title": "国内铝锭社会库存去化速度加快", "source": "SMM", "impact": "positive"},
        {"title": "氧化铝价格上涨推升电解铝生产成本", "source": "阿拉丁", "impact": "neutral"},
    ],
}


def generate_demo():
    today_dir = get_today_dir()
    all_data = {}

    for metal_id, metal in METALS.items():
        kline = gen_kline(metal)
        last = kline[-1] if kline else {"close": 0}
        base_price = last["close"]
        contracts = get_nearest_contracts(metal_id, 3)

        # Contract data
        contracts_data = {}
        tick = metal["tick_size"]
        for c in contracts:
            cp = base_price + random.uniform(-tick * 10, tick * 15)
            prev = cp - random.uniform(-tick * 5, tick * 5)
            contracts_data[c] = {
                "code": c,
                "last_price": round(cp, 2),
                "open": round(cp - random.uniform(-tick * 3, tick * 3), 2),
                "high": round(cp + tick * random.randint(1, 8), 2),
                "low": round(cp - tick * random.randint(1, 8), 2),
                "prev_close": round(prev, 2),
                "volume": random.randint(30000, 200000),
                "open_interest": random.randint(100000, 500000),
                "change": round(cp - prev, 2),
                "change_pct": round((cp - prev) / prev * 100, 2) if prev else 0,
                "asks": [{"price": round(cp + tick * i, 2), "volume": random.randint(50, 500)} for i in range(1, 6)],
                "bids": [{"price": round(cp - tick * i, 2), "volume": random.randint(50, 500)} for i in range(1, 6)],
            }

        # Indicators
        total_inv = random.randint(80000, 200000)
        inv_change = random.randint(-3000, 2000)
        indicators = {
            "total_inventory": total_inv,
            "registered_inventory": int(total_inv * random.uniform(0.4, 0.7)),
            "qualified_inventory": int(total_inv * random.uniform(0.6, 0.85)),
            "inventory_change": inv_change,
            "open_interest": random.randint(200000, 700000),
            "daily_volume": random.randint(50000, 200000),
            "position_ratio": round(random.uniform(0.8, 1.4), 2),
            "warehouses": random.randint(8, 20),
        }

        # Predictions
        vol_ratio = tick * 50 / max(base_price, 1)
        scenarios = [
            {"delivery_rate": "5%", "label": "极低交割", "adj": 2.5, "prob": 0.08,
             "description": "多头资金高度控盘,仓单严重不足,可能出现逼仓行情"},
            {"delivery_rate": "15%", "label": "低交割", "adj": 1.2, "prob": 0.20,
             "description": "持仓集中度较高,卖方交割意愿不强,期货维持升水"},
            {"delivery_rate": "30%", "label": "正常交割", "adj": 0.1, "prob": 0.40,
             "description": "期现回归正常,基差在合理区间收敛,交割顺畅"},
            {"delivery_rate": "50%", "label": "高交割", "adj": -0.8, "prob": 0.22,
             "description": "卖方交割意愿强烈,库存充裕,期货贴水压力增大"},
            {"delivery_rate": "70%+", "label": "极高交割", "adj": -1.8, "prob": 0.10,
             "description": "大量仓单集中交割,空头主导市场,价格承压明显"},
        ]
        predictions = []
        for s in scenarios:
            price = base_price * (1 + s["adj"] * vol_ratio)
            prob = s["prob"]
            if inv_change < 0:
                prob = prob * 1.2 if s["adj"] > 0 else prob * 0.85
            direction = "偏多" if s["adj"] > 0.5 else "偏空" if s["adj"] < -0.5 else "中性"
            predictions.append({
                "delivery_rate": s["delivery_rate"], "label": s["label"],
                "price": round(price, 2), "probability": round(prob, 4),
                "direction": direction, "description": s["description"],
            })
        total_prob = sum(p["probability"] for p in predictions)
        for p in predictions:
            p["probability"] = round(p["probability"] / total_prob, 4)

        best = max(predictions, key=lambda x: x["probability"])

        # News
        news = [dict(n, date=datetime.now().strftime("%H:%M"), url="") for n in NEWS_POOL.get(metal_id, [])]

        all_data[metal_id] = {
            "metal_id": metal_id,
            "metal_name": metal["name"],
            "metal_name_en": metal["name_en"],
            "unit": metal["unit"],
            "color": metal["color"],
            "kline": {"metal_id": metal_id, "data": kline, "updated_at": datetime.now().isoformat()},
            "realtime": {"last_price": base_price},
            "contracts": {"metal_id": metal_id, "contracts": contracts, "data": contracts_data, "updated_at": datetime.now().isoformat()},
            "indicators": {"metal_id": metal_id, "indicators": indicators, "updated_at": datetime.now().isoformat()},
            "news": {"metal_id": metal_id, "news": news, "updated_at": datetime.now().isoformat()},
            "predictions": {
                "metal_id": metal_id, "contract": contracts[0],
                "base_price": base_price, "predictions": predictions,
                "analysis": {
                    "most_likely_scenario": best["label"],
                    "most_likely_rate": best["delivery_rate"],
                    "expected_price": best["price"],
                    "inventory_status": "偏紧" if inv_change < 0 else "平衡",
                    "overall_direction": best["direction"],
                },
                "updated_at": datetime.now().isoformat(),
            },
        }

    output_file = os.path.join(today_dir, "market_data.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({
            "fetch_time": datetime.now().isoformat(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "metals": all_data,
        }, f, ensure_ascii=False, indent=2)

    print(f"✅ 演示数据已生成: {output_file}")
    print(f"   文件大小: {os.path.getsize(output_file) / 1024:.1f} KB")
    print(f"\n   用浏览器打开 index.html 查看 (需要通过 HTTP 服务器)")
    print(f"   python -m http.server 8080")
    return output_file


if __name__ == "__main__":
    generate_demo()
