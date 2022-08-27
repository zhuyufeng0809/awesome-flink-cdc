### 架构

#### 并发与线程同步

### 优势与特性

* 原生支持PolarDB MySQL
  详见`https://ververica.github.io/flink-cdc-connectors/master/content/about.html#supported-connectors`
  
* 简化部署，减少资源占用
  使用Debezium做CDC需要部署Kafka集群，需要部署生产者（Debezium）和消费者（Flink Job）两个应用
  使用Flink CDC无需部署Kafka，只需要一个应用（Flink Job）
  
* 消息格式定制化
  自定义`DebeziumDeserializationSchema`
  
* 幂等写入
  ```sql
  INSERT .. ON DUPLICATE KEY UPDATE ..
  ```

* 微批写入
  与Flink Jdbc Connector Sink行为一致，内部缓存的数据**条数超过阈值**或者**时间超过阈值**，则Flush。为了提高批量写入的性能，Mysql Url需要添加`rewriteBatchedStatements=true`参数

* 并发写入
  多线程并发写入数据库，每个线程负责一个Table，Table之间互不影响
  
* 细粒度的并发控制
  使用读写锁。同一个表DML之间的并发控制由阻塞队列内部保证，DML执行线程获取读锁，

* 自动创建目标表
  当新增同步表时，无需手动创建目标表

* 处理删除和清空
  将Delete和Truncate操作转换为更新逻辑删除字段

* 完美处理时间类型
  可支持纳秒级别的时间同步

* 动态增减表

### Todo

* Schema evolution
  可直接拿到源库DDL变更语句，更简洁优雅地处理Schema变更
  Drop table
  唯一键变更
  表名变更
  truncate
  
* Json配置

* 架构图

### To be resolved

1. end to end exactly-once
   如果想完美处理DDL（保证端到端的精准一次），需要Flink框架层面的支持，用户层面无法*完美*处理DDL
   
2. 数据倾斜
   源表更新频率不同会导致数据倾斜
   根据唯一键字段或主键字段将消息相对地均匀分发到下游算子，以此缓解数据倾斜
   * 如果同时存在唯一键和主键，优先使用唯一键
   * 如果唯一键和主键都没有，则该表所有数据都发送到subtask-0处理
     
   此方案处理DDL时可能有数据一致性问题，并且端到端精准一次同样很难保证
  
3. 动态修改配置
   使用广播流或者集成`Nacos`
   
4. 处理长事务
   长事务和检查点的一致性问题
   
5. 支持写入`Hudi`

6. 消息复用
   支持双写到MySQL和Kafka，既可以满足Cdc场景，其它实时作业又可以从Kafka复用Cdc消息
   
6. 完善日志