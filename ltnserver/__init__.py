import os
import sys
import json
import pyhdb

from signal import signal, SIGINT
from flask import Flask
from flask.ext.cors import CORS


static_folder = "static"
if len(sys.argv) >= 2:
    static_folder = sys.argv[1]

app = Flask(__name__, static_folder=static_folder)
CORS(app, supports_credentials=True)

SERVER_ROOT = os.path.dirname(os.path.realpath(__file__))

SECRET_KEY = 'development key'
app.config.from_object(__name__)

context = (SERVER_ROOT + '/../certificate.crt', SERVER_ROOT + '/../certificate.key')

connection = None


def init():
    try_reconnecting()


def init_training():
    signal(SIGINT, handle_signal)
    training.init()


def reset_connection():
    global connection
    if connection is not None:
        try:
            connection.close()
        except Exception, e:
            print e
    connection = None


def get_connection():
    return connection


def try_reconnecting():
    try:
        global connection
        with open(SERVER_ROOT + "/../secrets.json") as f:
            secrets = json.load(f)
        connection = pyhdb.connect(
            host=secrets['host'],
            port=secrets['port'],
            user=secrets['username'],
            password=secrets['password']
        )
    except Exception, e:
        print e


def handle_signal(the_signal, frame):
    print "Gracefully shutting down. Please wait..."
    training.should_continue = False
    training.model_thread.join()
    print "Done. Goodbye."
    sys.exit(0)


import ltnserver.server
import ltnserver.training
