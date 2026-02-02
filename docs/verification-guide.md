# Metad 恢复机制验证指南

## 一、验证环境准备

### 1.1 搭建测试集群

#### 方式 1: 使用现有配置（推荐）

项目已包含 3 节点 metad 配置：
- `etc/nebula-metad-0.conf` - 端口 9559
- `etc/nebula-metad-1.conf` - 端口 9560  
- `etc/nebula-metad-2.conf` - 端口 9561

启动命令：
```bash
# 启动所有 metad 节点
./scripts/nebula.service start metad-0
./scripts/nebula.service start metad-1
./scripts/nebula.service start metad-2

# 或者使用 standalone 模式（单机测试）
./build/standalone/nebula-metad --flagfile=etc/nebula-metad.conf
```

#### 方式 2: Docker 集群

```bash
# 使用 docker-compose 启动 3 节点集群
docker-compose -f docker/docker-compose.yml up -d
```

### 1.2 验证集群状态

```bash
# 检查节点是否运行
ps aux | grep nebula-metad

# 检查日志
tail -f logs/nebula-metad.*.log.INFO.*

# 使用 nebula-console 连接
./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -u root -p password
```

## 二、验证测试用例

### 2.1 测试 1: WAL 文件损坏恢复

**目的**: 验证当 WAL 文件损坏时，metad 能否自动检测和修复

**步骤**:
1. 确保集群正常运行
2. 停止一个 metad 节点
3. 手动损坏 WAL 文件（在文件末尾添加垃圾数据）
4. 重启节点
5. 观察日志，确认恢复过程

**预期结果**:
- 节点能够启动
- 日志中出现 "truncate" 或 "invalid wal" 警告
- WAL 文件被截断到最后一个有效位置
- 节点能够从 Leader 同步缺失的日志

**验证脚本**:
```bash
./scripts/test-metad-recovery.sh
# 或手动执行测试 1
```

**关键日志**:
```
[WARNING] Invalid wal <path>, truncate from offset <pos>
[INFO] Reset lastLogId <id> to be the committedLogId <id>
```

### 2.2 测试 2: RocksDB 数据损坏恢复

**目的**: 验证当 RocksDB 数据损坏时，metad 能否从其他节点同步恢复

**步骤**:
1. 确保集群正常运行
2. 停止一个 metad 节点
3. 删除部分 RocksDB 数据文件（.sst 文件）
4. 重启节点
5. 观察日志，确认同步过程

**预期结果**:
- 节点能够启动
- 节点检测到数据不一致
- 从 Leader 同步缺失的数据
- 如果日志落后太多，接收快照

**验证脚本**:
```bash
./scripts/test-metad-recovery.sh
# 或手动执行测试 2
```

**关键日志**:
```
[INFO] About to replicate logs in range [<start>, <end>] to all peer hosts
[INFO] Receive snapshot from <leader>
```

### 2.3 测试 3: 节点正常重启恢复

**目的**: 验证节点正常重启后能否正确恢复状态

**步骤**:
1. 记录当前 WAL 文件数量和状态
2. 正常停止节点
3. 重启节点
4. 检查 WAL 文件和状态是否一致

**预期结果**:
- 节点能够正常启动
- WAL 文件数量保持一致
- 从 WAL 和 RocksDB 恢复状态
- 加入 Raft 组并同步最新日志

**验证脚本**:
```bash
./scripts/test-metad-recovery.sh
# 或手动执行测试 3
```

**关键日志**:
```
[INFO] Load part <spaceId>, <partId> from disk
[INFO] There are <n> peer hosts, lastLogId <id>, committedLogId <id>
```

### 2.4 测试 4: 多数派故障测试

**目的**: 验证当多数派节点故障时，系统的行为

**步骤**:
1. 在 3 节点集群中停止 2 个节点
2. 观察剩余节点的行为
3. 恢复节点，观察恢复过程

**预期结果**:
- 剩余节点无法成为 Leader（无法形成多数派）
- 集群无法处理写请求
- 恢复节点后，集群自动恢复

**验证脚本**:
```bash
./scripts/test-metad-recovery.sh
# 或手动执行测试 4
```

**关键日志**:
```
[WARNING] Cannot become leader, insufficient peers
[INFO] Waiting for leader heartbeat
```

## 三、手动验证步骤

### 3.1 准备测试数据

```bash
# 使用 nebula-console 创建测试数据
./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -u root -p password <<EOF
CREATE SPACE test_space(partition_num=10, replica_factor=3);
USE test_space;
CREATE TAG person(name string, age int);
INSERT VERTEX person(name, age) VALUES 100:("Alice", 25);
INSERT VERTEX person(name, age) VALUES 200:("Bob", 30);
EOF
```

### 3.2 执行损坏测试

#### 测试 WAL 损坏

```bash
# 1. 停止节点
./scripts/nebula.service stop metad-0

# 2. 损坏 WAL 文件
META_DIR="data/meta/meta0/nebula/0/wal"
LAST_WAL=$(ls -t $META_DIR/*.wal | head -1)
echo "CORRUPTED" >> $LAST_WAL

# 3. 重启节点
./scripts/nebula.service start metad-0

# 4. 检查日志
tail -f logs/nebula-metad.*.log.INFO.* | grep -i "truncate\|corrupt\|wal"
```

#### 测试数据损坏

```bash
# 1. 停止节点
./scripts/nebula.service stop metad-1

# 2. 删除部分数据文件
META_DIR="data/meta/meta1/nebula/0/data"
rm -f $META_DIR/*.sst

# 3. 重启节点
./scripts/nebula.service start metad-1

# 4. 检查日志
tail -f logs/nebula-metad.*.log.INFO.* | grep -i "sync\|replicate\|snapshot"
```

### 3.3 验证数据一致性

```bash
# 查询测试数据，验证是否一致
./build/bin/nebula-console -addr 127.0.0.1 -port 9669 -u root -p password <<EOF
USE test_space;
FETCH PROP ON person 100, 200;
EOF
```

## 四、监控和日志分析

### 4.1 关键日志位置

- **启动日志**: `logs/nebula-metad.*.log.INFO.*`
- **错误日志**: `logs/nebula-metad.*.log.ERROR.*`
- **警告日志**: `logs/nebula-metad.*.log.WARNING.*`

### 4.2 关键日志模式

#### WAL 恢复相关
```
Invalid wal <path>, truncate from offset <pos>
Reset lastLogId <id> to be the committedLogId <id>
Found empty wal file
```

#### 日志同步相关
```
About to replicate logs in range [<start>, <end>]
AppendEntries success
Last matched log id: <id>
```

#### 快照恢复相关
```
Receive snapshot from <leader>
Begin to receive the snapshot
Receive all snapshot, committedLogId <id>
```

#### 选举相关
```
Become leader at term <term>
Vote for <candidate> at term <term>
```

### 4.3 监控指标

使用 HTTP 接口监控集群状态：
```bash
# 获取集群信息
curl http://127.0.0.1:19559/status

# 获取 Leader 信息
curl http://127.0.0.1:19559/leader
```

## 五、故障场景模拟

### 5.1 场景 1: 单节点 WAL 损坏

```bash
# 模拟步骤
1. 正常运行集群
2. 停止 metad-0
3. 损坏 WAL: echo "BAD" >> data/meta/meta0/nebula/0/wal/0000000000000000001.wal
4. 重启 metad-0
5. 观察恢复过程
```

### 5.2 场景 2: 单节点数据完全丢失

```bash
# 模拟步骤
1. 正常运行集群
2. 停止 metad-1
3. 删除数据目录: rm -rf data/meta/meta1/nebula/0/data/*
4. 重启 metad-1
5. 观察从 Leader 同步过程
```

### 5.3 场景 3: 网络分区

```bash
# 模拟步骤（需要多机环境）
1. 正常运行集群
2. 使用 iptables 阻断节点间通信
3. 观察 Leader 选举和日志同步
4. 恢复网络
5. 观察数据同步
```

## 六、验证检查清单

- [ ] 集群能够正常启动
- [ ] 所有节点能够正常加入 Raft 组
- [ ] WAL 文件损坏能够自动检测和修复
- [ ] RocksDB 数据损坏能够从其他节点同步恢复
- [ ] 节点重启后能够正确恢复状态
- [ ] 多数派故障时系统行为正确
- [ ] 数据一致性得到保证
- [ ] 日志记录完整，便于问题排查

## 七、常见问题

### Q1: 节点无法启动怎么办？

**检查**:
1. 查看错误日志: `logs/nebula-metad.*.log.ERROR.*`
2. 检查端口是否被占用: `netstat -tlnp | grep <port>`
3. 检查数据目录权限: `ls -la data/meta/`

### Q2: 节点无法加入集群怎么办？

**检查**:
1. 检查网络连通性: `ping <meta_ip>`
2. 检查配置中的 meta_server_addrs 是否正确
3. 检查集群 ID 是否一致

### Q3: 数据同步很慢怎么办？

**可能原因**:
1. 网络带宽不足
2. 数据量太大，需要接收快照
3. 节点资源不足（CPU/内存/磁盘）

**解决**:
1. 检查网络状况
2. 查看日志确认是否在接收快照
3. 增加节点资源

## 八、总结

通过以上验证测试，可以确认：

1. ✅ **Metad 损坏时数据可以自动恢复**（前提是多数派节点存活）
2. ✅ **WAL 文件损坏能够自动检测和修复**
3. ✅ **RocksDB 数据损坏能够从其他节点同步恢复**
4. ✅ **节点重启后能够正确恢复状态**
5. ⚠️ **全集群损坏需要手动从备份恢复**

验证完成后，建议：
- 定期备份 metad 数据
- 监控集群健康状态
- 设置告警机制
- 定期进行恢复演练
