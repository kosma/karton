# -*- coding: utf-8 -*-

"""
Minimal Redis protocol implementation.

Reply translation table (Redis <- Python):

    Status Reply <- true, Status
    Error Reply <- Exception, Error
    Integer Reply <- int
    Bulk Reply <- str
    NULL Bulk Reply <- None
    Multi Bulk Reply <- list

Protocol parsing is handled by hiredis at the moment.
"""

import collections

class Status(object):

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return '<Status reply +%s>' % self.message


class Error(object):

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return '<Error reply -%s>' % self.message


OK = Status('OK')


def python_to_redis(response):
    if response is True:
        return '+OK\r\n'
    elif response is False:
        return '-ERR\r\n'
    elif isinstance(response, Status):
        return '+%s\r\n' % response.message
    elif isinstance(response, (Error, AssertionError)):
        return '-%s\r\n' % response.message
    elif isinstance(response, Exception):
        return '-ERR %s\r\n' % repr(response)
    elif isinstance(response, (int, long)):
        return ':%d\r\n' % response
    elif isinstance(response, str):
        return '$%d\r\n%s\r\n' % (len(response), response)
    elif response is None:
        return '$-1\r\n'
    elif isinstance(response, collections.Iterable):
        return ('*%d\r\n' % len(response)) + ''.join(map(python_to_redis, response))
    else:
        raise ValueError("don't know how to handle %s" % repr(response))
