环境准备：
1.linux单机环境，用户ouzezh

2.安装zookeeper
127.0.0.1 z1
mkdir -p /home/ouzezh/bigdata/data/zookeeper
/home/ouzezh/bigdata/zookeeper-3.x

3.安装kafka
127.0.0.1 k1
mkdir -p /home/ouzezh/bigdata/data/kafka
/home/ouzezh/bigdata/confluent-5.x

4.安装connector
mkdir -p /home/ouzezh/bigdata/data/connectors
mkdir -p /home/ouzezh/bigdata/connectors
解压程序到/home/ouzezh/bigdata/connectors

5.测试
echo "test" >> /home/ouzezh/bigdata/data/connectors/in.log