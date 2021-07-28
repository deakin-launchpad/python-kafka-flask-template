#!/usr/bin/env python
# encoding: utf-8
import json
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from flask_sqlalchemy import SQLAlchemy
from json import dumps,loads
from kafka import KafkaProducer, KafkaConsumer
from threading import Event
import signal
from flask_kafka import FlaskKafka
import datetime
from json import JSONEncoder
app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

INTERRUPT_EVENT = Event()

bus = FlaskKafka(INTERRUPT_EVENT,
                 bootstrap_servers=",".join(["localhost:29092"]),
                 group_id="consumer-grp-id"
                 )


def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)


class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

def on_send_success(record_metadata):
    print(record_metadata.topic)


def on_send_error(excp):
    print('I am an errback: ' + excp)

@bus.handle('ARE_YOU_OK')
def topic_ARE_YOU_OK(msg):
    print("consumed {} from ARE_YOU_OK".format(msg))

@bus.handle('I_AM_LIVE')
def topic_I_AM_LIVE(msg):
    print("consumed {} from I_AM_LIVE".format(msg))


@app.route('/', methods=['GET'])
def default():
    return "Welcome to Digital Twin Tempelate"

@app.route('/areYouOkay', methods=['GET'])
def areYouOkay():
    demoData = {
      "topic": "ARE_YOU_OK",
      "messages": "",
      "attributes": 1,
      "timestamp": datetime.datetime.now()
    }
    demoData = DateTimeEncoder().encode(demoData)
    producer.send("ARE_YOU_OK",demoData).add_callback(on_send_success).add_errback(on_send_error)
    return "Sent data succesfully"

if __name__ == '__main__':
    bus.run()
    listen_kill_server()
    app.run(host="localhost", port=9000, debug=True)