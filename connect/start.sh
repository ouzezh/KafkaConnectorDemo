#!/usr/bin/env sh

sh $dir/stop.sh

dir=$(cd $(dirname $0);pwd)
#/home/ouzezh/bigdata/confluent-5.1.0/bin/connect-standalone \
$dir/bin/connect-standalone \
  $dir/config/connect-standalone.properties \
  $dir/config/connect-file-source.properties $dir/config/connect-file-sink.properties > /dev/null 2>&1 &

echo $! > $dir/run.pid
echo "start pid:"
cat $dir/run.pid
