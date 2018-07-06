### 概述
本工程是为了验证分布式监控而做的demo

### 开源组件或清单
- kafka: 消息中间件，负责大批量日志的存储转发
- Zipkin： 负责微服务调用链监控
- ElasticSearch: 负责存储交易报文日志

另外说明下两个库：
- github.com/openzipkin/zipkin-go-opentracing：基于opentracing标准，生成符合zipkin格式的报文日志
- github.com/bsm/sarama-cluster： 基于sarama扩展的kafka多集群高性能客户端

### 工程说明
```
├── cmd
│   ├── consumer        # kafka消费者
│   ├── esclient        # 一个读取es的客户端
│   └── producer        # kafka生产者
├── config
│   └── config.go       # 读取配置文件
├── config.json         # 配置参数
├── monitor
│   ├── accesslog.go    # 应用模拟日志
│   ├── producer.go     # kafka producer
│   ├── randstring.go   # rand string

```

### 关键流程
1. producer建立http server，等待交易发起，等交易收到后，随机生产特定大小的报文日志插入到kafka，同时将tracing日志发送至zipkin server上，完成http 返回；
2. consumer监听kafka topic上的消息，取出消息，并对消息进行反序列化,批量插入到elasticsearch中；

### 调优参数
1. go http client: 限定MaxIdleConnsPerHost个数，否则，client会发起大量goroutine
```
http.Client{
    Transport: &http.Transport{
        MaxIdleConnsPerHost: c.MaxIdleConnsPerHost,
        MaxIdleConns:        c.MaxIdleConnsPerHost * 2,
    },
}
```
2. Kafka producer参数：
```
Producer.Flush.Frequency
```

3. ElasticSearch参数：
```
sarma库client参数：
c.Consumer.Return.Errors = true
c.Group.Return.Notifications = true
c.ChannelBufferSize = 2048
c.Net.KeepAlive = 300
c.Net.MaxOpenRequests = 256

同时，修改index mapping 参数：
"number_of_shards": 30,   # 主分片，分片多，性能高
"number_of_replicas": 1,  # 复制分片，分片多，IO占比高
"refresh_interval": "60s", # 插入刷新时间间隔
"translog.durability": "async", # translog异步方式
"translog.sync_interval": "30s", # 缓冲到磁盘时间间隔
"translog.flush_threshold_size": "1024m" # translog大小阀值

es server配置文件参数：
thread_pool.bulk.queue_size: 1024
```
