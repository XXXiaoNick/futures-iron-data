#!/usr/bin/env python3
"""
数据管理工具
============
- 列出所有可用数据日期
- 清理过期数据
- 生成 data/index.json 供前端自动发现可用日期
"""

import os
import sys
import json
import shutil
from datetime import datetime, timedelta
from config import DATA_DIR

def list_dates():
    """列出所有有数据的日期"""
    if not os.path.exists(DATA_DIR):
        print("暂无数据")
        return []

    dates = []
    for d in sorted(os.listdir(DATA_DIR)):
        json_path = os.path.join(DATA_DIR, d, "market_data.json")
        if os.path.isdir(os.path.join(DATA_DIR, d)) and os.path.exists(json_path):
            size = os.path.getsize(json_path) / 1024
            dates.append({"date": d, "size_kb": round(size, 1)})

    if dates:
        print(f"\n可用数据日期 ({len(dates)} 天):")
        print("─" * 40)
        for item in dates:
            print(f"  📅 {item['date']}  ({item['size_kb']:.1f} KB)")
        print()
    else:
        print("暂无数据, 请先运行 python fetcher.py")

    return dates


def generate_index():
    """生成 data/index.json 供前端自动获取可用日期列表"""
    dates = list_dates()
    index_path = os.path.join(DATA_DIR, "index.json")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump({
            "available_dates": [d["date"] for d in dates],
            "latest": dates[-1]["date"] if dates else None,
            "generated_at": datetime.now().isoformat(),
        }, f, ensure_ascii=False, indent=2)
    print(f"✓ 索引已生成: {index_path}")
    return dates


def clean_old(keep_days=30):
    """清理超过 keep_days 天的旧数据"""
    if not os.path.exists(DATA_DIR):
        print("无数据目录")
        return

    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    removed = 0

    for d in sorted(os.listdir(DATA_DIR)):
        full_path = os.path.join(DATA_DIR, d)
        if os.path.isdir(full_path) and d < cutoff:
            shutil.rmtree(full_path)
            print(f"  🗑 已删除 {d}")
            removed += 1

    if removed:
        print(f"\n共清理 {removed} 天数据 (保留最近 {keep_days} 天)")
    else:
        print(f"无需清理 (所有数据均在 {keep_days} 天内)")


def show_summary(date_str=None):
    """显示某日数据摘要"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")

    json_path = os.path.join(DATA_DIR, date_str, "market_data.json")
    if not os.path.exists(json_path):
        print(f"找不到 {date_str} 的数据")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"\n{'═' * 50}")
    print(f"  数据摘要: {date_str}")
    print(f"  采集时间: {data.get('fetch_time', '未知')}")
    print(f"{'═' * 50}")

    for mid, m in data.get("metals", {}).items():
        kline = m.get("kline", {}).get("data", [])
        rt = m.get("realtime", {})
        contracts = m.get("contracts", {}).get("contracts", [])
        news_count = len(m.get("news", {}).get("news", []))
        pred = m.get("predictions", {}).get("predictions", [])

        price = rt.get("last_price") or (kline[-1]["close"] if kline else 0)
        print(f"\n  {m.get('metal_name', mid)} ({mid})")
        print(f"    价格: {price} {m.get('unit', '')}")
        print(f"    K线: {len(kline)} 条")
        print(f"    合约: {', '.join(contracts)}")
        print(f"    新闻: {news_count} 条")
        print(f"    预测: {len(pred)} 个情景")

    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python manage.py <命令>")
        print("  list      - 列出所有可用日期")
        print("  index     - 生成索引文件")
        print("  clean     - 清理30天前的旧数据")
        print("  clean N   - 清理N天前的旧数据")
        print("  summary   - 显示今日数据摘要")
        print("  summary YYYY-MM-DD - 显示指定日期摘要")
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "list":
        list_dates()
    elif cmd == "index":
        generate_index()
    elif cmd == "clean":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        clean_old(days)
    elif cmd == "summary":
        date = sys.argv[2] if len(sys.argv) > 2 else None
        show_summary(date)
    else:
        print(f"未知命令: {cmd}")
