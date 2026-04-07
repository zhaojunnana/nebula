#!/bin/bash
set -e

NEBULA_HOME="/usr/local/nebula"
cd "$NEBULA_HOME"

# 记录当前所有 nebula 进程的启动命令
echo "=== Step 0: 记录当前启动命令 ==="
CMDS=$(ps aux | grep -E 'nebula-(metad|graphd|storaged)' | grep -v grep | awk '{for(i=11;i<=NF;i++) printf "%s ", $i; print ""}')
if [ -z "$CMDS" ]; then
    echo "没有找到运行中的 nebula 进程，退出"
    exit 1
fi
echo "$CMDS"

# 保存到数组
mapfile -t CMD_ARRAY <<< "$CMDS"

# Step 1: 关闭所有 nebula 进程
echo ""
echo "=== Step 1: 关闭所有 nebula 进程 ==="
pkill -f 'nebula-(metad|graphd|storaged)' || true
sleep 2

# 确认已全部关闭
REMAINING=$(pgrep -f 'nebula-(metad|graphd|storaged)' || true)
if [ -n "$REMAINING" ]; then
    echo "仍有进程存活，强制 kill..."
    pkill -9 -f 'nebula-(metad|graphd|storaged)' || true
    sleep 1
fi
echo "所有 nebula 进程已关闭"

# Step 2: 用 build/bin 覆盖 bin
echo ""
echo "=== Step 2: 用 build/bin/ 覆盖 bin/ ==="
for f in nebula-metad nebula-graphd nebula-storaged; do
    if [ -f "build/bin/$f" ]; then
        cp -f "build/bin/$f" "bin/$f"
        echo "  已更新: bin/$f"
    else
        echo "  跳过(不存在): build/bin/$f"
    fi
done
echo "可执行文件更新完成"

# Step 3: 按原命令重启，统一使用 bin/ 下的可执行文件
echo ""
echo "=== Step 3: 重启所有 nebula 进程 ==="
for cmd in "${CMD_ARRAY[@]}"; do
    [ -z "$cmd" ] && continue
    # 将 ./build/bin/ 或 build/bin/ 替换为 bin/
    cmd=$(echo "$cmd" | sed 's|\./build/bin/|bin/|g; s|build/bin/|bin/|g')
    echo "  启动: $cmd"
    $cmd &
done

sleep 2
echo ""
echo "=== 当前 nebula 进程 ==="
ps aux | grep -E 'nebula-(metad|graphd|storaged)' | grep -v grep
echo ""
echo "升级完成!"
