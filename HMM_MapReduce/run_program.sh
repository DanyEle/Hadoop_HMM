mvn clean package
hadoop jar target/hmm-0.0.1-SNAPSHOT.jar it.cnr.isti.pad.HMM input output
rm -r output
#hadoop fs -get output ./
#ls output
