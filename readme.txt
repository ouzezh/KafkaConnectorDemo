环境准备：
1.linux单机环境，用户ouzezh

2.安装zookeeper
x z1
mkdir -p /home/ouzezh/bigdata/data/zookeeper
/home/ouzezh/bigdata/zookeeper-3.x

3.安装confluent 4.1.2
(1)安装
x k1
mkdir -p /home/ouzezh/bigdata/data/kafka
/home/ouzezh/bigdata/confluent-5.x
(2)运行Schema Registry
x s1

5.安装connector
mkdir -p /home/ouzezh/bigdata/connectors
解压程序到/home/ouzezh/bigdata/connectors

6.测试
echo "test" >> /home/ouzezh/bigdata/data/connectors/in.log