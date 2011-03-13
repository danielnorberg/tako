# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import urllib
import os

import processing

from syncless import coio
from syncless import patch
patch.patch_socket()

from httpserver import HttpServer
from testcase import TestCase


class TestHttpServer(TestCase):
    def GET(self, start_response, path, body, env):
        start_response("200 OK", [('Content-Type', 'text/html')])
        return ["TestHttpServer"]

    def testServer(self):
        s = HttpServer(listener=('', 4711), handlers=(('/', {'GET':self.GET}),))
        t = coio.stackless.tasklet(s.serve)()
        coio.stackless.schedule()
        stream = urllib.urlopen('http://127.0.0.1:4711/')
        body = stream.read()
        stream.close()
        assert body == "TestHttpServer"
        t.kill()


    def testPerf(self):
        s = HttpServer(listener=('', 4711), handlers=(('/', {'GET':self.GET}),))
        t = coio.stackless.tasklet(s.serve)()
        coio.stackless.schedule()
        def ab():
            os.system('ab -k -n 10000 -c 10 http://127.0.0.1:4711/')
        p = processing.Process(target=ab)
        p.start()
        while p.isAlive():
            coio.sleep(1)
        t.kill()

if __name__ == '__main__':
    import unittest
    unittest.main()
