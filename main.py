#!/usr/bin/env python

import os
import sys
from gcm import GCM
from pymongo import MongoClient
from pika_pack import Listener


RABBIT_URL = os.getenv('RABBIT_URL', None)
assert RABBIT_URL

MONGO_URL = os.getenv('MONGO_URL', None)
assert MONGO_URL

GCM_API_KEY = os.getenv('GCM_API_KEY', None)
assert GCM_API_KEY

EXCHANGE = 'gpio_broadcast'

DEVICE_KEY = 'stall_monitor'

mongo_db = MongoClient(MONGO_URL).get_default_database()
gcm = GCM(GCM_API_KEY)


def send_gcm(message):
    gcm_tokens = [g['token'] for g in mongo_db.gcms.find()]
    response = gcm.json_request(registration_ids=gcm_tokens, data=message)
    # TODO: handle bad tokens in the database


def send_notifications(message):
    send_gcm(message)


def main():
    rabbit_listener = Listener(
        rabbit_url=RABBIT_URL,
        exchange=EXCHANGE,
        routing_key=DEVICE_KEY,
        request_action=send_notifications)

    try:
        rabbit_listener.start()
    except:
        sys.exit(1)


if __name__ == '__main__':
    main()
