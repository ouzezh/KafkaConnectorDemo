#!/usr/bin/env sh

dir=$(cd $(dirname $0);pwd)

>${dir}/logs/log4j.log

>${dir}/../in1.log
>${dir}/../in2.log
rm -f ${dir}/../out.log

rm -f /home/ouzezh/bigdata/data/connectors/connect.source.offsets

/home/ouzezh/bigdata/confluent-4.1.2/bin/kafka-topics --zookeeper localhost:2181 --delete --topic connect-test
/home/ouzezh/bigdata/confluent-4.1.2/bin/kafka-topics --zookeeper localhost:2181 --create --topic connect-test --partitions 2 --replication-factor 1
