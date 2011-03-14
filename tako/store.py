# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import pytc as tc
from syncless import coio

from utils import debug
from utils import timestamper

BUFFER_THRESHOLD = 4096

class Store(object):
    def __init__(self, filepath, auto_commit_interval=0.5):
        self.operation_counter = 0
        super(Store, self).__init__()
        debug.log('filepath: %s', filepath)
        self.filepath = filepath
        self.db = tc.BDB()
        self.db.tune(0, 0, 1024**2*10, 0, -1, 0)
        self.flusher = None
        self.auto_commit_interval = auto_commit_interval
        self.pack_timestamp = timestamper.pack
        self.unpack_timestamp = timestamper.unpack

    def open(self):
        self.db.open(self.filepath, tc.BDBOWRITER | tc.BDBOCREAT)
        if self.auto_commit_interval:
            self.begin()
            self.flusher = coio.stackless.tasklet(self.__flush)()

    def close(self):
        if self.auto_commit_interval:
            self.flusher.kill()
            self.flusher = None
            self.commit()
        self.db.close()

    def __flush(self):
        while True:
            debug.log('Committing %d operations', self.operation_counter)
            self.commit()
            self.operation_counter = 0
            self.begin()
            coio.sleep(self.auto_commit_interval)

    def __unpack_timestamped_data(self, data):
        if len(data) > BUFFER_THRESHOLD:
            value = buffer(data, 8)
        else:
            value = data[8:]
        timestamp = self.unpack_timestamp(data[0:8])
        return timestamp, value

    def __jump(self, cur, start):
        keylen = len(start)
        cur.jump(start)
        key = cur.key()
        if keylen:
            while not key[:keylen] > start:
                cur.next()
                key = cur.key()
        return key

    def __range(self, cur, start, end):
        keys = []
        try:
            endlen = len(end)
            key = self.__jump(cur, start)
            while key[:endlen] <= end:
                keys.append(key)
                cur.next()
                key = cur.key()
        except KeyError:
            pass
        return keys

    def set(self, key, timestamp, value):
        self.operation_counter += 1
        self.db.put(key, self.pack_timestamp(timestamp))
        self.db.putcat(key, value)

    def get(self, key):
        try:
            data = self.db.get(key)
            return self.__unpack_timestamped_data(data)
        except Exception:
            pass
        return (None, None)

    def get_key_range(self, start_key, end_key):
        cur = self.db.curnew()
        return self.__range(cur, start_key, end_key)

    def abort(self):
        self.db.tranabort()

    def begin(self):
        self.db.tranbegin()

    def commit(self):
        self.db.trancommit()

