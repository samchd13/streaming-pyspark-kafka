# streaming-pyspark-kafka

This code takes counts number of tweets in the batch and number of the most used words.

# To start you need:
### Download and extract an Apache Kafka binary from: 
https://kafka.apache.org/downloads.html
### Start zookeeper service in a kafka folder(where you extract the binary):
$ bin/zookeeper-server-start.sh config/zookeeper.properties
### Start kafka service in a kafka folder:
$ bin/kafka-server-start.sh config/server.properties
### Create a topic in kafka:
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic TOPIC_NAME
### Get yout keys for Twitter Streaming API:
Example in config.json
### Run a program to collect tweets:
$ python /*path_to_folder*/twitter_stream.py <TOPIC_NAME>

For example:
$ python /*path_to_folder*/twitter_stream.py test
### To check if the data is adding into the Kafka:
$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic TOPIC_NAME
### Download Apache Spark binary from:
https://spark.apache.org/downloads.html
### Start Spark streaming:
$ $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 /*path_to_folder*/word_count.py <broker> <topics list> <branch duration>
 
For example:
$ $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 /*path_to_folder*/word_count.py localhost:9092 test 10

