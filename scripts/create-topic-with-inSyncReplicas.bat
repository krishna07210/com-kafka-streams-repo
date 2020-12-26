cd D:\KAFKA_HOME\kafka-cluster\kafka_Node1
cd windows

echo Hi
@echo off
echo Please provide following Input to create the Topic :
echo -----------------------------------------------------
echo Pleae provide the Topic name - 
set /p topic="" 
echo Pleae provide the Replication Factor - 
set /p rf="" 
echo Please provide the Partition No -
set /p part=""
echo Please provide the inSync Replicas Count -
set /p insync=""
echo kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%  --config min.insync.replicas=%insync%
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor %rf% --partitions %part% --topic %topic%  --config min.insync.replicas=%insync%   >>  C:\Krishna\My_Learnings\KAFKA_HOME\TopicStatusLog.log
