# coding=utf-8
"""
celery worker -A jobs --concurrency=4 --loglevel=info  --without-gossip
"""
import logging
import string
import random
from celery import Celery
import redis
from confluent_kafka import Producer
import time

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

# client = KafkaClient(hosts='localhost:9092')
# topic = client.topics['test']
# producer = topic.get_producer()
# app = Celery(
#     'jobs', broker='redis://:55f7b56ab5e13921e0935e92@dataTask01:63799/10')
app = Celery('jobs', broker='redis://:@localhost:6379/0')
r = redis.StrictRedis.from_url('redis://:@localhost:6379/0')


def base_str():
    return string.digits + string.letters


@app.task()
def add_like():
    global r
    keylist = [random.choice(base_str()) for i in range(10)]
    key = "".join(keylist)
    start = time.time()
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    p.produce('test', key)
    p.flush()
    end = time.time()
    r.lpush('timecost', end - start)
