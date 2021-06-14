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
app = Flask(__name__)

app.config["MONGO_URI"] = "mongodb://localhost:27017/digital_twin_dev"
mongo_client = PyMongo(app)
mongo_db = mongo_client.db

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://deakin_launchpad:xmtD0T6hNOj5@digital-twin-dev.cqznqsycghdg.ap-southeast-2.rds.amazonaws.com:5432/digital_twin_dev'
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = 'secret string'

postgres_db = SQLAlchemy(app)

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


@bus.handle('ARE_YOU_OK')
def topic_ARE_YOU_OK(msg):
    print("consumed {} from ARE_YOU_OK".format(msg))

@bus.handle('I_AM_LIVE')
def topic_I_AM_LIVE(msg):
    print("consumed {} from I_AM_LIVE".format(msg))


@app.route('/', methods=['GET'])
def default():
    return "Welcome to Digital Twin Tempelate"

if __name__ == '__main__':
    bus.run()
    listen_kill_server()
    app.run(host="localhost", port=9000, debug=True)