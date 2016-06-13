import os
import json


SERVER_ROOT = os.path.dirname(os.path.realpath(__file__))


def get_root_path(path):
    return SERVER_ROOT + '/../' + path


def get_settings(key):
    with open(get_root_path('secrets.json')) as f:
        return json.load(f).get(key)
