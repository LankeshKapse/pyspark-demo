# Start zookeeper
START %KAFKA_HOME%\bin\windows\zookeeper-server-start.bat C:\Users\Lucky\Documents\learning\project-interview-2023\pyspark-demo\scripts\kafka-scripts\zookeeper.properties

# Start kafka
START %KAFKA_HOME%\bin\windows\kafka-server-start.bat C:\Users\Lucky\Documents\learning\project-interview-2023\pyspark-demo\scripts\kafka-scripts\server.properties

# Console producer
START %KAFKA_HOME%\bin\windows\kafka-console-producer.bat --topic sensor --bootstrap-server localhost:9092
START %KAFKA_HOME%\bin\windows\kafka-console-producer.bat --topic sensor.status --bootstrap-server localhost:9092

#Console consumer
START %KAFKA_HOME%\bin\windows\kafka-console-consumer.bat -bootstrap-server localhost:9092 -topic sensor -from-beginning

# list topic in kafka
%KAFKA_HOME%\bin\windows\kafka-topics.bat --bootstrap-server=localhost:9092 --list

# Create topic
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic sensor --replication-factor 1 --partitions 1  --bootstrap-server localhost:9092


