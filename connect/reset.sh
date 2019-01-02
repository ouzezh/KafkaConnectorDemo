>../in1.log
>../in2.log
rm -f ../out.log
rm -f /home/ouzezh/bigdata/data/connectors/connect.source.offsets
/home/ouzezh/bigdata/confluent-5.1.0/bin/kafka-topics --zookeeper localhost:2181 --delete --topic connect-test
/home/ouzezh/bigdata/confluent-5.1.0/bin/kafka-topics --zookeeper localhost:2181 --create --topic connect-test --partitions 2 --replication-factor 1
