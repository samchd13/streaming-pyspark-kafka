from __future__ import print_function
import argparse
import json
from kafka import SimpleProducer, KafkaClient, SimpleClient, KafkaProducer
import tweepy
import time


def main():
    parser = argparse.ArgumentParser(description="Scrape twitter data and put it into Kafka.")
    parser.add_argument("t", nargs=1, help="topics list")
    args = parser.parse_args()
    topic_name = args.t[0]

    with open('config.json') as config_data:
        config = json.load(config_data)

    auth = tweepy.auth.OAuthHandler(config['consumer_key'], config['consumer_secret'])
    auth.set_access_token(config['access_token'], config['access_token_secret'])

    stream = tweepy.Stream(auth=auth, listener=TwitterListener(topic_name))
    stream.filter(locations=[-180, -90, 180, 90], languages=['ru'])


class TwitterListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka"""

    def __init__(self, topic_name):
        super().__init__()
        self.topic_name = topic_name
        self.count = 0
        self.time_start = time.time()
        client = SimpleClient("localhost:9092")
        self.producer = SimpleProducer(client, async=True,
                                       batch_send_every_n=1000,
                                       batch_send_every_t=10)

    def on_status(self, status):
        self.count += 1
        msg = status.text.encode('utf-8')
        try:
            self.producer.send_messages(self.topic_name, msg)
        except Exception as e:
            print(e)
            return False
        # except KeyboardInterrupt:
        #     print("For time: {}".format(time.time() - self.time_start))
        #     print("Messages in this batch: {}".format(self.count))
        return True

    # def on_data(self, raw_data):
    #     try:
    #         self.producer.send_messages(self.topic_name, raw_data.encode("utf-8"))
    #     except Exception as e:
    #         print(e)
    #         print("tut")
    #         return False
    #     return True

    def on_error(self, status_code):
        print('An error has occured! Status code = %s' % status_code)
        return True  # Don't kill the stream

    def on_timeout(self):
        print("Timeout!")
        return True  # Don't kill the stream


if __name__ == '__main__':
    main()
