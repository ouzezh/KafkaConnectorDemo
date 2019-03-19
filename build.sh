#!/usr/bin/env sh

gradle build copyJars

cp build/libs/target-0.1.jar connect/share/java/connect
rm -f connect/share/java/kafka/*.jar
cp build/libs/ext/* connect/share/java/kafka