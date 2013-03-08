#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 
# Twisted-based Karton server.

import os
import sys
import logging
logger = logging.getLogger('twisted_karton')

from twisted.python import log, usage
from twisted.internet import defer, protocol
import hiredis

import karton.protocol
import karton.server


def reactor():
    from twisted.internet import reactor as r
    return r


class RedisProtocol(protocol.Protocol):

    def __init__(self, server, addr):
        self.client = server.new_client(addr)
        self.reader = hiredis.Reader()

    def connectionLost(self, reason):
        self.client.die()
        del self.client
        del self.reader
        
    def dataReceived(self, data):
        self.reader.feed(data)
        request = self.reader.gets()
        if request is not False:
            response = self.client.do(request)
            raw = karton.protocol.python_to_redis(response)
            self.transport.write(raw)


class RedisProtocolFactory(protocol.ServerFactory):

    protocol = RedisProtocol

    def startFactory(self):
        self.server = karton.server.Server()

    def buildProtocol(self, addr):
        return self.protocol(self.server, addr)

class Options(usage.Options):

    optParameters = [
        ["port", "p", 6379, "server port", int]
    ]


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
            format='[%(process)d] %(asctime)s [%(name)s] %(levelname)s: %(message)s')
    observer = log.PythonLoggingObserver()
    observer.start()

    config = Options()
    try:
        config.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    protocol.Factory.noisy = True

    reactor().listenTCP(config['port'], RedisProtocolFactory())
    reactor().callWhenRunning(logger.info, "The server is now ready to accept connections on port %d", config['port'])
    reactor().run()

if __name__ == '__main__':
    main()
