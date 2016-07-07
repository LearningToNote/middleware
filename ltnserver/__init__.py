import sys
import pyhdb
import json

from signal import signal, SIGINT
from flask import Flask, Response, redirect, url_for
from flask.ext.cors import CORS

from settings import get_settings, get_root_path

static_folder = "static"
if len(sys.argv) >= 2:
    static_folder = sys.argv[1]

app = Flask(__name__, static_folder=static_folder)
CORS(app, supports_credentials=True)

SECRET_KEY = get_settings('secrets').get('development_key')
app.config.from_object(__name__)

context = (get_root_path('certificate.crt'), get_root_path('certificate.key'))

connection = None


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
    if connection is None:
        try_reconnecting()
    return connection


def execute_prepared(query, params, commit=False):
    cursor = get_connection().cursor()
    psid = cursor.prepare(query)
    ps = cursor.get_prepared_statement(psid)
    cursor.execute_prepared(ps, [params])
    if commit:
        get_connection().commit()


def try_reconnecting():
    try:
        global connection
        db = get_settings('database')
        connection = pyhdb.connect(
            host=db['host'],
            port=db['port'],
            user=db['username'],
            password=db['password']
        )
    except Exception, e:
        print e


def handle_signal(s, _):
    print 'Gracefully shutting down. Please wait...'
    training.should_continue = False
    training.model_thread.join()
    print 'Done. Goodbye.'
    sys.exit(0)


def respond_with(response):
    return Response(json.dumps(response), mimetype='application/json')


@app.route('/')
def home():
    return redirect(url_for('static', filename='index.html'))


import user
import types
import tasks
import documents
import formats
import training
import prediction
