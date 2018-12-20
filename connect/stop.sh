#!/usr/bin/env sh

dir=$(cd $(dirname $0);pwd)

if [ -f $dir/run.pid ]
then
 echo "kill pid:"
 cat $dir/run.pid
 kill -9 $(cat $dir/run.pid)
else
 echo "no pid file skip kill"
fi

