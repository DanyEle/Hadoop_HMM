#!/bin/bash
mvn clean package
INPUT_FOLDER=$1
echo $INPUT_FOLDER
hadoop jar target/hmm-0.0.1-SNAPSHOT.jar it.cnr.isti.pad.HMM $INPUT_FOLDER output
rm -r output
hadoop fs -get output ./
ls output
