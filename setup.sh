#!/bin/bash
# ═══════════════════════════════════════════════
#  贵金属期货交易终端 - 安装 & 定时任务设置
# ═══════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "═══════════════════════════════════════════════"
echo "  贵金属期货交易终端 - 环境设置"
echo "═══════════════════════════════════════════════"

# 1. 安装 Python 依赖
echo ""
echo "[1/4] 安装 Python 依赖..."
pip install akshare requests beautifulsoup4 --break-system-packages -q 2>/dev/null || \
pip install akshare requests beautifulsoup4 -q
echo "  ✓ 依赖安装完成"

# 2. 创建数据目录
echo ""
echo "[2/4] 创建数据目录..."
mkdir -p "$SCRIPT_DIR/data"
echo "  ✓ 数据目录: $SCRIPT_DIR/data/"

# 3. 测试运行一次
echo ""
echo "[3/4] 运行首次数据采集 (可能需要 1-2 分钟)..."
cd "$SCRIPT_DIR"
python fetcher.py
echo "  ✓ 首次采集完成"

# 4. 设置定时任务 (每天 9:00)
echo ""
echo "[4/4] 设置定时任务..."

CRON_CMD="0 9 * * 1-5 cd $SCRIPT_DIR && /usr/bin/python3 fetcher.py >> $SCRIPT_DIR/cron.log 2>&1"

# 检查是否已有该任务
if crontab -l 2>/dev/null | grep -q "fetcher.py"; then
    echo "  ⚠ 已存在定时任务, 跳过添加"
else
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
    echo "  ✓ 已添加 cron 任务: 每个交易日 9:00 自动采集"
fi

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ 设置完成!"
echo ""
echo "  使用方式:"
echo "    1. 用浏览器打开 index.html"
echo "    2. 选择日期查看对应数据"
echo "    3. 手动采集: python fetcher.py"
echo ""
echo "  数据结构:"
echo "    data/"
echo "      2026-02-26/"
echo "        market_data.json    <- 每日所有品种数据"
echo "      2026-02-27/"
echo "        market_data.json"
echo "      ..."
echo ""
echo "  定时任务: 每周一至周五 09:00 自动运行"
echo "  日志文件: $SCRIPT_DIR/fetcher.log"
echo "═══════════════════════════════════════════════"
