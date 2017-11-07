from __future__ import print_function
import argparse
import json
import re
from stop_words import get_stop_words
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def main():
    parser = argparse.ArgumentParser(description="Gets twitter data from Kafka and work with it.")
    parser.add_argument("broker", nargs=1, help="broker name")
    parser.add_argument("topics", nargs="+", help="topics list")
    parser.add_argument("batch", nargs=1, type=int, help="Batch duration for StreamingContext")
    args = parser.parse_args()

    broker = args.broker[0]
    topics = args.topics
    batch_duration = args.batch[0]

    print(broker, topics, type(batch_duration))

    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)

    # sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, batch_duration)

    # brokers, topics = 'localhost:9092', 'test2'
    kvs = KafkaUtils.createDirectStream(ssc, topics, {"metadata.broker.list": broker})

    stop_words = set()
    for w in get_stop_words('russian'):
        stop_words.add(w)

    text_pattern = r'[А-ЯЁа-яё]*'

    lines = kvs.map(lambda x: x[1])
    ssc.checkpoint("./checkpoint-tweet")

    lines.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()

    running_counts = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda x: x.lower() not in stop_words) \
        .filter(lambda x: x != '' and re.match(text_pattern, x).group() == x) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(update_func).transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
    # running_counts.count().map(lambda x: 'Tweets at all: %s' % x).pprint()
    running_counts.pprint()

    ssc.start()
    ssc.awaitTermination()


def update_func(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


if __name__ == "__main__":
    main()
