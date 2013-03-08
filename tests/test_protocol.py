# -*- coding: utf-8 -*-

from karton.protocol import python_to_redis, Status, Error


def test_python_to_redis():
    # http://redis.io/topics/protocol

    # Status reply
    assert python_to_redis(True) == '+OK\r\n'
    assert python_to_redis(Status('PONG')) == '+PONG\r\n'
    assert python_to_redis(Status('GOOD')) == '+GOOD\r\n'

    # Error reply
    assert python_to_redis(False) == '-ERR\r\n'
    assert python_to_redis(Error('WRONGTYPE')) == '-WRONGTYPE\r\n'
    assert python_to_redis(Error("ERR I'm a teapot")) == "-ERR I'm a teapot\r\n"

    # Integer reply
    assert python_to_redis(0) == ':0\r\n'
    assert python_to_redis(42) == ':42\r\n'
    assert python_to_redis(42L) == ':42\r\n'
    assert python_to_redis(36893488147419103232) == ':36893488147419103232\r\n'
    assert python_to_redis(36893488147419103232L) == ':36893488147419103232\r\n'

    # Bulk reply
    assert python_to_redis('') == '$0\r\n\r\n'
    assert python_to_redis('foo') == '$3\r\nfoo\r\n'
    assert python_to_redis('foo\0bar\r\n') == '$9\r\nfoo\0bar\r\n\r\n'

    # NULL bulk reply
    assert python_to_redis(None) == '$-1\r\n'

    # Multi-bulk reply
    assert python_to_redis(['foo', 'ba\nr', 'xyzzy']) == '*3\r\n$3\r\nfoo\r\n$4\r\nba\nr\r\n$5\r\nxyzzy\r\n'
    assert python_to_redis([True, False, 42, 'wat']) == '*4\r\n+OK\r\n-ERR\r\n:42\r\n$3\r\nwat\r\n'
    assert python_to_redis([True, ['foo', 'bar'], True]) == '*3\r\n+OK\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+OK\r\n'

    # Not supported: NULL multi-bulk reply (*-1)
