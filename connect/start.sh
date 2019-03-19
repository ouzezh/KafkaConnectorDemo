#!/usr/bin/env sh

dir=$(cd $(dirname $0);pwd)
sh $dir/stop.sh
>$dir/logs/log4j.log

$dir/bin/connect-standalone $dir/config/connect-standalone.properties \
  $dir/config/connect-file-source.properties $dir/config/connect-file-sink.properties > /dev/null 2>&1 &

# distributed start worker
#$dir/bin/connect-distributed $dir/config/connect-distributed.properties &

echo $! > $dir/run.pid
echo "start pid:"
cat $dir/run.pid

# distributed start connector
#sleep 15
#curl -X PUT -H "Content-Type: application/json" localhost:8083/connectors/local-file-source/config --data "$(cat $dir/config/connect-file-source.json)"
#curl -X PUT -H "Content-Type: application/json" localhost:8083/connectors/local-file-sink/config --data "$(cat $dir/config/connect-file-sink.json)"