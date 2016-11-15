#!/bin/bash

ROOT=`dirname $0`
cd $ROOT/..
mvn package install -DcreateChecksum=true -DskipTests
mkdir -p releases/download
mv target/releases/elasticsearch-analysis-jieba-*-bin.zip releases/download
