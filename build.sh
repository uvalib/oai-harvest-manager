#!/bin/bash


PATH=/usr/local/bin:$PATH

mvn -s settings.xml -Dmaven.test.skip=true  dependency:copy-dependencies compile package

