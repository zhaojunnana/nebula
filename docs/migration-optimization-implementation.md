# Nebula Graph 集群迁移优化实现总结

## 一、已完成的工作

### 1.1 源码分析

已完成对 metad 损坏恢复机制和集群迁移功能的深入分析：

1. **恢复机制分析** (`docs/metad-recovery-and-migration-analysis.md`)
   - Raft 日志同步恢复机制
   - WAL 文件扫描与修复
   - 快照恢复机制
   - 备份恢复机制

2. **迁移功能分析**
   - RestoreProcessor 备份恢复迁移
   - MetaHttpReplaceHostHandler 在线替换主机
   - BalanceJob 数据平衡

### 1.2 优化方案设计

设计了统一的集群迁移管理器 (`MigrationManager`)，提供：

1. **迁移前检查**
   - 源集群健康检查
   - 目标集群容量检查
   - 网络连通性检查
   - 版本兼容性检查

2. **迁移进度跟踪**
   - 实时进度查询
   - 步骤详情记录
   - 错误信息记录

3. **迁移管理**
   - 开始迁移
   - 查询进度
   - 回滚迁移
   - 取消迁移

### 1.3 代码实现

已实现以下文件：

1. **MigrationManager.h** (`src/meta/processors/admin/MigrationManager.h`)
   - 迁移管理器接口定义
   - 迁移计划、进度、结果数据结构
   - 迁移检查结果结构

2. **MigrationManager.cpp** (`src/meta/processors/admin/MigrationManager.cpp`)
   - 迁移检查实现
   - 迁移执行框架
   - 进度跟踪实现
   - 回滚和取消功能

3. **MigrationProcessor.h** (`src/meta/processors/admin/MigrationProcessor.h`)
   - 迁移 Processor 接口（待完善）

## 二、核心功能说明

### 2.1 MigrationManager 接口

```cpp
class MigrationManager {
  // 检查是否可以执行迁移
  MigrationCheckResult checkMigration(const MigrationPlan& plan);
  
  // 开始迁移
  folly::Future<MigrationResult> startMigration(const MigrationPlan& plan);
  
  // 查询迁移进度
  StatusOr<MigrationProgress> getProgress(int32_t migrationId);
  
  // 回滚迁移
  folly::Future<Status> rollbackMigration(int32_t migrationId);
  
  // 取消迁移
  folly::Future<Status> cancelMigration(int32_t migrationId);
  
  // 列出所有迁移任务
  std::vector<MigrationProgress> listMigrations();
};
```

### 2.2 迁移流程

```
1. 准备阶段 (PREPARING)
   - 验证迁移计划
   - 执行迁移前检查
   - 初始化迁移进度

2. 迁移阶段 (MIGRATING)
   - 迁移 meta 数据
   - 迁移 storage 数据
   - 更新主机映射

3. 验证阶段 (VERIFYING)
   - 验证数据一致性
   - 验证服务可用性

4. 完成阶段 (COMPLETED)
   - 清理临时数据
   - 更新迁移状态
```

### 2.3 迁移检查项

1. **源集群健康检查**
   - 检查源主机是否存活
   - 检查源集群是否正常

2. **目标集群容量检查**
   - 检查目标主机是否存在
   - 检查磁盘空间是否充足
   - 检查 CPU/内存资源

3. **网络连通性检查**
   - Ping 目标主机
   - 检查端口是否开放

4. **版本兼容性检查**
   - 检查源集群版本
   - 检查目标集群版本
   - 验证版本兼容性

## 三、待完成的工作

### 3.1 Thrift 接口定义

需要定义以下 Thrift 接口：

```thrift
// interface/gen-cpp2/meta.thrift

struct HostMapping {
  1: HostAddr from,
  2: HostAddr to,
}

enum MigrationStrategy {
  FULL = 1,
  INCREMENTAL = 2,
}

struct MigrationPlan {
  1: list<HostMapping> hostMappings,
  2: MigrationStrategy strategy,
  3: bool autoRollback,
  4: string description,
}

enum MigrationStatus {
  PREPARING = 1,
  MIGRATING = 2,
  VERIFYING = 3,
  COMPLETED = 4,
  FAILED = 5,
  ROLLING_BACK = 6,
}

struct MigrationProgress {
  1: i32 migrationId,
  2: MigrationStatus status,
  3: i32 totalSteps,
  4: i32 completedSteps,
  5: string currentStep,
  6: i64 startTime,
  7: i64 estimatedCompletionTime,
  8: map<string, string> details,
  9: string errorMessage,
}

struct MigrationCheckResult {
  1: bool canMigrate,
  2: list<string> warnings,
  3: list<string> errors,
}

struct MigrationReq {
  1: MigrationPlan plan,
}

struct MigrationResp {
  1: nebula.ErrorCode code,
  2: i32 migrationId,
  3: MigrationCheckResult checkResult,
}

service MetaService {
  MigrationResp migrate(1: MigrationReq req),
  MigrationProgress getMigrationProgress(1: i32 migrationId),
  nebula.ErrorCode rollbackMigration(1: i32 migrationId),
  nebula.ErrorCode cancelMigration(1: i32 migrationId),
  list<MigrationProgress> listMigrations(),
}
```

### 3.2 MigrationProcessor 实现

需要实现 `MigrationProcessor::process()` 方法：

```cpp
void MigrationProcessor::process(const cpp2::MigrationReq& req) {
  auto plan = req.get_plan();
  
  // 检查迁移
  auto checkResult = migrationManager_->checkMigration(plan);
  
  if (!checkResult.canMigrate) {
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_MIGRATION_CHECK_FAILED;
    resp_.checkResult_ref() = checkResult;
    onFinished();
    return;
  }
  
  // 开始迁移
  auto future = migrationManager_->startMigration(plan);
  future.thenValue([this](MigrationResult result) {
    resp_.code_ref() = result.success ? 
        nebula::cpp2::ErrorCode::SUCCEEDED : 
        nebula::cpp2::ErrorCode::E_MIGRATION_FAILED;
    resp_.migrationId_ref() = result.migrationId;
    onFinished();
  });
}
```

### 3.3 实际迁移逻辑实现

需要实现以下实际迁移逻辑：

1. **Meta 数据迁移**
   - 使用 `RestoreProcessor` 的逻辑
   - 替换主机地址
   - 更新分区信息

2. **Storage 数据迁移**
   - 使用 `BalanceJob` 的逻辑
   - 逐个分区迁移
   - 验证数据一致性

3. **增量迁移支持**
   - 第一阶段：创建目标空间和分区
   - 第二阶段：同步数据
   - 第三阶段：切换流量
   - 第四阶段：清理旧集群

### 3.4 HTTP API 实现

需要创建 HTTP handler 提供 RESTful API：

```cpp
class MetaHttpMigrationHandler {
  // POST /migration/start
  void startMigration(const MigrationPlan& plan);
  
  // GET /migration/progress/:id
  MigrationProgress getProgress(int32_t id);
  
  // POST /migration/rollback/:id
  void rollbackMigration(int32_t id);
  
  // POST /migration/cancel/:id
  void cancelMigration(int32_t id);
  
  // GET /migration/list
  std::vector<MigrationProgress> listMigrations();
};
```

### 3.5 测试用例

需要添加以下测试：

1. **单元测试**
   - MigrationManager 测试
   - MigrationProcessor 测试
   - 迁移检查测试

2. **集成测试**
   - 小规模迁移测试
   - 大规模迁移测试
   - 故障恢复测试

3. **性能测试**
   - 迁移性能测试
   - 并发迁移测试

## 四、使用示例

### 4.1 检查迁移

```cpp
MigrationPlan plan;
plan.hostMappings = {
  {HostAddr("192.168.1.1", 9779), HostAddr("192.168.2.1", 9779)},
  {HostAddr("192.168.1.2", 9779), HostAddr("192.168.2.2", 9779)},
};
plan.strategy = MigrationStrategy::FULL;
plan.autoRollback = true;

auto checkResult = migrationManager->checkMigration(plan);
if (!checkResult.canMigrate) {
  LOG(ERROR) << "Cannot migrate: " << checkResult.toString();
  return;
}
```

### 4.2 开始迁移

```cpp
auto future = migrationManager->startMigration(plan);
future.thenValue([](MigrationResult result) {
  if (result.success) {
    LOG(INFO) << "Migration completed: " << result.migrationId;
  } else {
    LOG(ERROR) << "Migration failed: " << result.errorMessage;
  }
});
```

### 4.3 查询进度

```cpp
auto progress = migrationManager->getProgress(migrationId);
if (progress.ok()) {
  auto p = progress.value();
  LOG(INFO) << "Progress: " << p.getProgressPercent() << "%";
  LOG(INFO) << "Current step: " << p.currentStep;
}
```

### 4.4 回滚迁移

```cpp
auto future = migrationManager->rollbackMigration(migrationId);
future.thenValue([](Status status) {
  if (status.ok()) {
    LOG(INFO) << "Migration rolled back successfully";
  } else {
    LOG(ERROR) << "Rollback failed: " << status.toString();
  }
});
```

## 五、后续优化方向

### 5.1 性能优化

1. **并行迁移**
   - 支持多个分区并行迁移
   - 优化网络传输

2. **增量迁移优化**
   - 减少数据传输量
   - 优化同步策略

### 5.2 功能增强

1. **迁移可视化**
   - Web UI 展示迁移进度
   - 迁移历史记录

2. **自动化脚本**
   - 提供迁移脚本工具
   - 支持一键迁移

3. **迁移验证**
   - 自动数据一致性检查
   - 性能对比测试

## 六、总结

已完成的工作：

1. ✅ 深入分析了 metad 恢复机制和迁移功能
2. ✅ 设计了统一的迁移管理器架构
3. ✅ 实现了 MigrationManager 核心功能框架
4. ✅ 提供了迁移进度跟踪和回滚机制

待完成的工作：

1. ⏳ 定义 Thrift 接口
2. ⏳ 实现 MigrationProcessor
3. ⏳ 实现实际迁移逻辑
4. ⏳ 实现 HTTP API
5. ⏳ 添加测试用例

当前实现提供了一个完整的迁移管理框架，可以在此基础上继续完善具体的迁移逻辑和接口定义。
