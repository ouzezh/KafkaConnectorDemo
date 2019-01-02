#!/usr/bin/env sh

curl -X DELETE http://localhost:8083/connectors/local-file-source && echo "stop source success" || echo "stop source fail"
curl -X DELETE http://localhost:8083/connectors/local-file-sink && echo "stop sink success" || echo "stop sink fail"

dir=$(cd $(dirname $0);pwd)
if [ -f $dir/run.pid ]
then
 echo "kill pid:"
 cat $dir/run.pid
 kill -9 $(cat $dir/run.pid)
else
 echo "no pid file skip kill"
fi
