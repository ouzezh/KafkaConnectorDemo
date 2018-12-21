#!/usr/bin/env sh
curl -X DELETE -H "Content-Type: application/json" http://localhost:8083/connectors/local-file-source && echo "stop success" || echo "stop fail"

dir=$(cd $(dirname $0);pwd)
if [ -f $dir/run.pid ]
then
 echo "kill pid:"
 cat $dir/run.pid
 kill -9 $(cat $dir/run.pid)
else
 echo "no pid file skip kill"
fi

