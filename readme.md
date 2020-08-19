## 运行程序
- 1.linux单机环境，用户ouzezh

- 2.安装zookeeper
```
127.0.0.1 z1
mkdir -p /home/ouzezh/bigdata/data/zookeeper
/home/ouzezh/bigdata/zookeeper-3.x
```

- 3.安装confluent 4.1.2
```
(1).安装
127.0.0.1 k1
mkdir -p /home/ouzezh/bigdata/data/kafka
/home/ouzezh/bigdata/confluent-5.x
(2).运行Schema Registry
127.0.0.1 s1
```

- 5.安装connector
```
(1).打包，拷贝jar和依赖到connect/share/java/connect及kafka
(2).拷贝connector到目录目录
mkdir -p /home/ouzezh/bigdata/connectors
解压程序到/home/ouzezh/bigdata/connectors
(3).运行start.sh
```

- 6.测试

echo "test" >> /home/ouzezh/bigdata/data/connectors/in.log

## Springboot集成Kafka

https://spring.io/projects/spring-kafka