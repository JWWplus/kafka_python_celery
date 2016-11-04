# coding=utf-8

import logging
from flask import Flask
from jobs import r
from confluent_kafka import Producer
import random
import string
import time
import gevent
from flask import copy_current_request_context
from gevent import monkey
monkey.patch_all()
FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
app = Flask(__name__)


@app.route('/like')
def count_like():
    # add_like.apply_async()
    keylist = [random.choice(base_str()) for i in range(10)]
    key = "".join(keylist)

    @copy_current_request_context
    def send_into_kafka(key):
        start = time.time()
        p = Producer({'bootstrap.servers': 'localhost:9092'})
        p.produce('test', key)
        p.flush()
        end = time.time()
        r.lpush('timecost', end - start)
    g = gevent.spawn(send_into_kafka, key)
    return ("like+1,add success! ")
    g.join()

@app.route('/avg')
def avg_second():
    sum_time = 0
    for i in r.lrange('timecost', 0, -1):
        sum_time += float(i)
    return str(sum_time / r.llen('timecost'))


def base_str():
    return string.digits + string.ascii_letters


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8088, debug=True)
