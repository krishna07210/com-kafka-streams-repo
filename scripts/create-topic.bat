cd D:\KAFKA_HOME\kafka-cluster\kafka-broker-0

@echo off

echo $$ Please enter the Following Inputs to create the Topic $$
echo ###########################################################
echo Please enter the TopicName ::
set /p topic=""
echo Please enter the Replication Factor ::
set /p rf=""
echo Please enter the No of Partitions ::
set /p part=""
echo kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%   
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%
