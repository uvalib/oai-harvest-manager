#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.19.0.7-1.el7_9.x86_64

mvn -s settings.xml -Dmaven.test.skip=true  dependency:copy-dependencies compile package

