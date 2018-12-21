#!/usr/bin/env sh

gradle build copyJars

cp build/libs/target-0.1.jar connect/share/java/connect
cp build/libs/libs/* connect/share/java/kafka