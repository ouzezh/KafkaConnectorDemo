#!/usr/bin/env sh

curl localhost:8083/connectors | jq
curl -X DELETE --connect-timeout 15 -m 30 http://localhost:8083/connectors/local-file-source && echo "stop source success" || echo "stop source fail"
curl -X DELETE --connect-timeout 15 -m 30 http://localhost:8083/connectors/local-file-sink && echo "stop sink success" || echo "stop sink fail"
curl localhost:8083/connectors | jq

dir=$(cd $(dirname $0);pwd)
if [ -f $dir/run.pid ]
then
 echo "kill pid:"
 cat $dir/run.pid
 kill -9 $(cat $dir/run.pid)
else
 echo "no pid file skip kill"
fi
