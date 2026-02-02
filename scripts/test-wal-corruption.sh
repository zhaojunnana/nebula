#!/bin/bash
# WAL 损坏测试脚本 - 一键测试 WAL 修复功能

set -e

# 配置
META_DATA_DIR="data/meta0"
WAL_DIR="${META_DATA_DIR}/nebula/0/wal/0"
LOG_DIR="logs"
META_PORT=9559  # metad 端口，Raft 端口是 port+1 (即 9560)
META_BIN="bin/nebula-metad"
CONFIG_FILE="etc/nebula-metad-0.conf"

# 端口冲突检查函数
check_port_conflicts() {
    local config_file="$1"
    if [ ! -f "$config_file" ]; then
        return 0  # 配置文件不存在，跳过检查
    fi
    
    # 提取 meta_server_addrs 中的端口
    local ports=$(grep -E "^--meta_server_addrs=" "$config_file" | sed 's/.*=//' | tr ',' '\n' | cut -d':' -f2 | sort -n)
    
    if [ -z "$ports" ]; then
        log_warn "Could not parse ports from config file, skipping port conflict check"
        return 0
    fi
    
    local prev_port=0
    local has_conflict=false
    
    for port in $ports; do
        if [ "$prev_port" -gt 0 ]; then
            local raft_port=$((prev_port + 1))
            if [ "$port" -le "$raft_port" ]; then
                log_error "⚠️  端口冲突检测: metad 端口 $prev_port 的 Raft 端口是 $raft_port，与下一个 metad 端口 $port 冲突！"
                log_error "   请确保 metad 端口之间至少间隔 2（例如：9559, 9561, 9563）"
                has_conflict=true
            fi
        fi
        prev_port=$port
    done
    
    if [ "$has_conflict" = true ]; then
        log_error "配置文件 $config_file 存在端口冲突，请修复后再运行脚本"
        return 1
    fi
    
    return 0
}

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 检查文件是否存在
check_file() {
    if [ ! -f "$1" ]; then
        log_error "File not found: $1"
        return 1
    fi
    return 0
}

# 检查目录是否存在
check_dir() {
    if [ ! -d "$1" ]; then
        log_error "Directory not found: $1"
        return 1
    fi
    return 0
}

# 步骤 0: 检查端口配置
log_step "0/8 检查端口配置..."
if ! check_port_conflicts "$CONFIG_FILE"; then
    exit 1
fi
log_info "端口配置检查通过"

# 步骤 1: 检查节点状态
log_step "1/8 检查节点状态..."

# 使用 PID 文件检测（最可靠）
PID_FILE="pids/nebula-metad-0.pid"
NODE_RUNNING=false
NODE_PID=""

if [ -f "$PID_FILE" ]; then
    NODE_PID=$(cat "$PID_FILE" 2>/dev/null | head -1)
    if [ -n "$NODE_PID" ] && kill -0 "$NODE_PID" 2>/dev/null; then
        log_info "Node is running (PID: $NODE_PID from $PID_FILE)"
        NODE_RUNNING=true
    else
        log_warn "PID file exists but process not running, cleaning up"
        rm -f "$PID_FILE"
    fi
else
    # 备用：通过进程名检测
    NODE_PID=$(pgrep -f "nebula-metad.*metad-0" 2>/dev/null | head -1)
    if [ -n "$NODE_PID" ]; then
        log_info "Node is running (PID: $NODE_PID, detected by pgrep)"
        NODE_RUNNING=true
    else
        log_warn "Node is not running"
    fi
fi

# 双重确认：检查端口是否被占用
if ss -tlnp 2>/dev/null | grep -q ":${META_PORT} "; then
    if [ "$NODE_RUNNING" = false ]; then
        log_error "Port $META_PORT is in use but node detection failed!"
        log_error "Please manually check: ss -tlnp | grep :${META_PORT}"
        exit 1
    fi
fi

# 步骤 2: 定位 WAL 文件
log_step "2/8 定位 WAL 文件..."
if ! check_dir "$META_DATA_DIR"; then
    log_error "Meta data directory not found: $META_DATA_DIR"
    log_info "Please check your configuration or start the node first"
    exit 1
fi

# 创建 WAL 目录（如果不存在）
mkdir -p "$WAL_DIR"

LAST_WAL=$(ls -t "$WAL_DIR"/*.wal 2>/dev/null | head -1)
if [ -z "$LAST_WAL" ]; then
    log_warn "No WAL files found. Creating a minimal test WAL file..."
    # 创建一个最小的有效 WAL 文件用于测试
    # LogID(8) + TermID(4) + ClusterID(8) + MsgLen(4) + Msg(0) + MsgLen(4) = 28 bytes
    printf '\x01\x00\x00\x00\x00\x00\x00\x00' > "$WAL_DIR/0000000000000000001.wal"  # LogID = 1
    printf '\x01\x00\x00\x00' >> "$WAL_DIR/0000000000000000001.wal"  # TermID = 1
    printf '\x00\x00\x00\x00\x00\x00\x00\x00' >> "$WAL_DIR/0000000000000000001.wal"  # ClusterID = 0
    printf '\x00\x00\x00\x00' >> "$WAL_DIR/0000000000000000001.wal"  # Message length = 0
    printf '\x00\x00\x00\x00' >> "$WAL_DIR/0000000000000000001.wal"  # Message length footer = 0
    LAST_WAL="$WAL_DIR/0000000000000000001.wal"
    log_info "Created test WAL file: $LAST_WAL"
fi

log_info "Target WAL file: $LAST_WAL"
ORIGINAL_SIZE=$(stat -f%z "$LAST_WAL" 2>/dev/null || stat -c%s "$LAST_WAL" 2>/dev/null)
log_info "Original size: $ORIGINAL_SIZE bytes"

# 步骤 3: 停止节点
log_step "3/8 停止节点..."
if [ "$NODE_RUNNING" = true ]; then
    if [ -z "$NODE_PID" ]; then
        log_error "NODE_RUNNING=true but NODE_PID is empty, aborting!"
        exit 1
    fi
    
    log_info "Stopping node (PID: $NODE_PID)..."
    kill "$NODE_PID" 2>/dev/null || true
    sleep 3
    
    # 确认节点已停止
    if kill -0 "$NODE_PID" 2>/dev/null; then
        log_warn "Process still running, force killing..."
        kill -9 "$NODE_PID" 2>/dev/null || true
        sleep 2
    fi
    
    # 最终确认
    if kill -0 "$NODE_PID" 2>/dev/null; then
        log_error "Failed to stop node (PID: $NODE_PID), aborting!"
        exit 1
    fi
    
    # 清理 PID 文件
    rm -f "$PID_FILE"
    log_info "Node stopped successfully"
else
    log_info "Node was not running, skipping stop step"
fi

# 步骤 4: 备份 WAL 文件
log_step "4/8 备份 WAL 文件..."
BACKUP_FILE="${LAST_WAL}.backup.$(date +%s)"
cp "$LAST_WAL" "$BACKUP_FILE"
log_info "Backup created: $BACKUP_FILE"

# 步骤 5: 损坏 WAL 文件
log_step "5/8 损坏 WAL 文件..."
echo "CORRUPTED_DATA_$(date +%s)" >> "$LAST_WAL"
echo "Random garbage data for testing" >> "$LAST_WAL"
# 添加随机二进制数据
dd if=/dev/urandom bs=100 count=1 >> "$LAST_WAL" 2>/dev/null || true

NEW_SIZE=$(stat -f%z "$LAST_WAL" 2>/dev/null || stat -c%s "$LAST_WAL" 2>/dev/null)
log_info "File corrupted: $ORIGINAL_SIZE -> $NEW_SIZE bytes (+$((NEW_SIZE - ORIGINAL_SIZE)) bytes)"

# 步骤 6: 重启节点
log_step "6/8 重启节点..."

# 检查可执行文件和配置文件
if [ ! -f "$META_BIN" ]; then
    log_error "Metad binary not found: $META_BIN"
    log_info "Please build the project first or adjust META_BIN variable"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Config file not found: $CONFIG_FILE"
    log_info "Please check CONFIG_FILE variable"
    exit 1
fi

# 启动节点（后台运行）
log_info "Starting node..."
RECOVERY_LOG="/tmp/metad-wal-recovery-$(date +%s).log"
nohup "$META_BIN" --flagfile="$CONFIG_FILE" > "$RECOVERY_LOG" 2>&1 &
META_PID=$!
log_info "Node started, PID: $META_PID"
log_info "Recovery log: $RECOVERY_LOG"

# 等待节点启动
log_info "Waiting for node to start..."
sleep 10

# 检查节点是否启动成功
if ! ps -p $META_PID > /dev/null 2>&1; then
    log_error "Node process died. Check log: $RECOVERY_LOG"
    tail -50 "$RECOVERY_LOG"
    exit 1
fi

# 步骤 7: 观察恢复过程
log_step "7/8 观察恢复过程..."
log_info "Monitoring recovery process (30 seconds)..."
log_info "Press Ctrl+C to stop monitoring early"
echo ""

# 监控恢复日志
timeout 30 tail -f "$RECOVERY_LOG" 2>/dev/null | grep --line-buffered -E "wal|truncate|corrupt|invalid|scan|recover|WARNING|ERROR|INFO.*part|INFO.*space" || true

# 等待一下让修复完成
sleep 3

# 检查修复结果
echo ""
log_step "检查修复结果..."

if [ -f "$LAST_WAL" ]; then
    FINAL_SIZE=$(stat -f%z "$LAST_WAL" 2>/dev/null || stat -c%s "$LAST_WAL" 2>/dev/null)
    echo ""
    echo -e "${GREEN}=== 修复结果 ===${NC}"
    echo "Original size:    $ORIGINAL_SIZE bytes"
    echo "Corrupted size:   $NEW_SIZE bytes"
    echo "Final size:       $FINAL_SIZE bytes"
    echo ""
    
    if [ "$FINAL_SIZE" -lt "$NEW_SIZE" ]; then
        TRUNCATED=$((NEW_SIZE - FINAL_SIZE))
        log_info "✓ WAL file was truncated by $TRUNCATED bytes (repaired)"
    elif [ "$FINAL_SIZE" -eq "$ORIGINAL_SIZE" ]; then
        log_warn "⚠ WAL file size matches original (may have been reset)"
    else
        log_warn "⚠ WAL file size unchanged (may not have been corrupted enough to trigger repair)"
    fi
else
    log_error "WAL file not found after recovery"
fi

# 检查日志文件中的修复消息
if [ -d "$LOG_DIR" ]; then
    echo ""
    log_step "检查日志文件中的修复消息..."
    REPAIR_MSGS=$(grep -h "truncate\|corrupt\|invalid wal" "$LOG_DIR"/nebula-metad.*.log.* 2>/dev/null | tail -5)
    if [ -n "$REPAIR_MSGS" ]; then
        echo -e "${GREEN}Found repair messages:${NC}"
        echo "$REPAIR_MSGS"
    else
        log_warn "No repair messages found in log files"
        log_info "Check recovery log: $RECOVERY_LOG"
    fi
fi

# 检查节点状态
echo ""
log_step "检查节点状态..."
if ps -p $META_PID > /dev/null 2>&1; then
    log_info "✓ Node is running (PID: $META_PID)"
    
    # 尝试检查 HTTP 服务
    if command -v curl > /dev/null 2>&1; then
        if curl -s http://127.0.0.1:19559/status > /dev/null 2>&1; then
            log_info "✓ HTTP service is responding"
        else
            log_warn "⚠ HTTP service not responding (may still be starting)"
        fi
    fi
else
    log_error "✗ Node process died"
    log_info "Check recovery log: $RECOVERY_LOG"
fi

# 总结
echo ""
echo -e "${GREEN}=== 测试完成 ===${NC}"
echo "Recovery log: $RECOVERY_LOG"
echo "Backup file:  $BACKUP_FILE"
echo "WAL file:     $LAST_WAL"
if [ -d "$LOG_DIR" ]; then
    echo "Log directory: $LOG_DIR"
fi
echo ""
log_info "To monitor recovery in real-time, run:"
echo "  tail -f $RECOVERY_LOG | grep -E 'wal|truncate|corrupt|invalid'"
echo ""
log_info "To check node status:"
echo "  ps aux | grep nebula-metad"
echo "  curl http://127.0.0.1:19559/status"
