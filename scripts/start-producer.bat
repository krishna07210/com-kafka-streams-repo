cd D:\KAFKA_HOME\kafka-cluster\kafka-broker-0\bin
cd windows

@echo off
echo $$ Please provide the TopicName for Produce the Messages    $$
echo ##############################################################
echo Please enter the TopicName ::
set /p topic=""
echo kafka-console-producer.bat --broker-list localhost:9092 --topic %topic% --property parse.key=true --property key.separator=":"

kafka-console-producer.bat --broker-list localhost:9092 --topic %topic%  --property parse.key=true --property key.separator=":"
