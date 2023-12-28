# NebulaGraph

## BZL-部分魔改功能

* 增加rang compact功能(时间可控的范围compact) **[相关代码](https://github.com/vesoft-inc/nebula/commit/08e03a58ba5ecbbee7f02b7c0f65bf342cf5e9e4)**
* 增加CDC(change data capture)功能 **[相关代码](https://github.com/vesoft-inc/nebula/commit/bd069aab314770e27fd51b8198e8bd753b6eb6d0)**
* Cypher语法表达式兼容: where条件中判断相等兼容`==`和`=`; where或return表达式兼容 `变量.属性`和`变量.$tag.属性` **[eq相关代码](https://github.com/vesoft-inc/nebula/commit/be138d2a9ba0aa761f034644448042380eda9286)** 和 **[prop相关代码](https://github.com/vesoft-inc/nebula/commit/dcd20426e487c8c1f07bb9e49a326299422613ba)**
* 查询方面针对优化:针对常用带`limit`的多度查询,开发stream(流式)调度执行器,优化查询性能 **本分支大部分Commits代码**


#### Rang Compact相关功能
console的space输入命令启动rang compact，60为compact运行时间，单位是分钟。执行时间可能会略长于指定时间，属于正常现象。
```
submit job rang_compact 60
```

```
show job <jobId> #查看compact运行状态
stop job <jobId> #停止compact，停止时间可能略有延迟
```

#### CDC相关功能
nebula-storaged.conf配置文件中增加
```
--enable_binlog=true #开启binlog功能
```
storage日志文件中会打印出实时变更日志，此功能的实现侵入了raft代码，会有部分性能损耗
```
I20230228 21:05:30.797564 3216573 Part.cpp:466] _BINLOG:{"spaceId":1, "tagId":2, "type":"VERTEX", "logID":27033, "termID":46, "vid": "18", "spaceType":"INT", "operate":"INSERT"}

I20230228 21:06:03.608285 3216575 Part.cpp:466] _BINLOG:{"spaceId":1, "tagId":2, "type":"VERTEX", "logID":27037, "termID":46, "vid": "18", "spaceType":"INT", "operate":"REMOVE"}

I20230228 21:06:26.705806 3216575 Part.cpp:466] _BINLOG:{"spaceId":1, "tagId":2, "type":"VERTEX", "logID":27018, "termID":42, "vid": "8", "spaceType":"INT", "operate":"REMOVE"}

I20230228 21:06:50.605970 3216575 Part.cpp:488] _BINLOG:{"spaceId":1, "edgeType":-4, "type":"EDGE", "logID":27021, "termID":42, "source": "2", "rank":3, "target": "1", "spaceType":"INT", "operate":"REMOVE"}

I20230228 21:06:50.606010 3216574 Part.cpp:488] _BINLOG:{"spaceId":1, "edgeType":4, "type":"EDGE", "logID":27038, "termID":44, "source": "1", "rank":3, "target": "2", "spaceType":"INT", "operate":"REMOVE"}

I20230228 21:06:50.606045 3216575 Part.cpp:488] _BINLOG:{"spaceId":1, "edgeType":-4, "type":"EDGE", "logID":27021, "termID":42, "source": "2", "rank":4, "target": "1", "spaceType":"INT", "operate":"REMOVE"}
```

#### Cypher语法表达式兼容
变量.$tag.prop兼容后的表达式，cypher原生语法模式下，针对多tag数据默认取第一个tag
```
//eq原语法
MATCH (v:player) WHERE v.player.name == "Tim Duncan" RETURN v;
//兼容语法
MATCH (v:player) WHERE v.player.name = "Tim Duncan" RETURN v;

//tag.prop原语法
MATCH (v1:player)-->(v2:player) WHERE v1.player.name in names RETURN v1, v2;
//兼容语法
MATCH (v1:player)-->(v2:player) WHERE v1.name in names RETURN v1, v2;
```

#### Stream调度引擎查询优化
nebula-graphd.conf配置文件加以下配置开启功能
```
--stream_executor_enable=true #开启stream
--stream_executor_batch_size=200 #每个流式执行批次的大小
```
使用如下类带limit的cypher会触发流式执行
```
MATCH (v:player)-[r:follow]-(e)-[r2]-(e2) where id(v)='player101' and r.degree=90 return v,r,e,r2,e2 limit 100
```
