cd D:\KAFKA_HOME\kafka-cluster\kafka_Node1
cd windows

@echo off
echo Please provide following Input to Consume the Topic --from-beginning  :
echo -----------------------------------------------------
echo Please provide the Topic name ::
set /p topic=""
echo kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %topic% --from-beginning 

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic %topic% --from-beginning 


pause

