cd D:\KAFKA_HOME\kafka-cluster\kafka-broker-0\bin
cd windows

@echo off
echo $$ Please provide the TopicName for Consumption    --from-beginning $$
echo #######################################################################
echo Please enter the TopicName ::
set /p topic=""
echo kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %topic% --from-beginning 

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %topic% --from-beginning 


pause

