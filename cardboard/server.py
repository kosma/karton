#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import math
import random
import signal
import fnmatch
import re
from collections import deque
from functools import partial
try:
    import cPickle as pickle
except ImportError:
    import pickle

from blist import blist, sorteddict

from .protocol import Status, Error, OK


def redis_slice(start, end):
    """Convert Redis start/end to Python slice."""
    return slice(int(start), (int(end)+1) or None)


def pass_value(value_type):
    """Pass first method value using key."""
    def decorator(method):
        def decorated(self, key, *args):
            try:
                value = self._ht_get(key, value_type)
                return method(self, value, *args)
            finally:
                self.ht[key] = value
                self._ht_check(key)
        decorated.__name__ = method.__name__
        return decorated
    return decorator


setmethod = pass_value(set)
hashmethod = pass_value(dict)
listmethod = pass_value(deque)


def pass_string(method):
    """Pass first method argument as HT string and handle return value appropriately."""
    def decorated(self, string, *args):
        new_value = None
        try:
            value = self._ht_get(key, value_type)
            retval, new_value = method(self, value, *args)
        finally:
            self.ht[key] = new_value
            self._ht_check(key)
            return new_value
    decorated.__name__ = method.__name__
    return decorated


class Client(object):

    def __init__(self, server, addr):
        self.server = server
        self.addr = addr

    def do(self, request):
        return self.server.do(*request)

    def die(self):
        # break circular references!
        del self.server


class Server(object):

    def __init__(self, dbs=16):
        self.dbs = [{} for index in xrange(dbs)]
        self.ht = self.dbs[0]

    def new_client(self, addr):
        return Client(self, addr)

    def do(self, *args):
        try:
            command = args[0]
            handler = getattr(self, '%s' % command.upper())
            result = handler(*args[1:])
        except Exception as exc:
            return exc
        else:
            return result

    def _ht_get(self, key, type):
        value = self.ht.get(key, type())
        assert isinstance(value, type)
        return value

    def _ht_check(self, key):
        if key in self.ht and not self.ht[key]:
            del self.ht[key]

    # Keys

    def DEL(self, *keys):
        """Fully compatible."""
        assert keys
        count = 0
        for key in keys:
            if key in self.ht:
                del self.ht[key]
                count += 1
        return count

    def DUMP(self, key):
        """Non-standard: uses Pickle instead of Redis format."""
        if key in self.ht:
            return pickle.dumps(self.ht[key])

    def EXISTS(self, key):
        """Fully compatible."""
        if key in self.ht:
            return 1
        else:
            return 0

    def EXPIRE(self, key, seconds):
        raise NotImplementedError

    def EXPIREAT(self, key, timestamp):
        raise NotImplementedError

    def KEYS(self, pattern):
        """Mostly compatible (wasn't tested extensively)."""
        pattern = re.sub(r'\\(.)', r'[\1]', pattern)
        return filter(partial(fnmatch.fnmatchcase, pat=pattern), self.ht.keys())

    def MOVE(self, key, db):
        raise NotImplementedError

    def PERSIST(self, key):
        raise NotImplementedError

    def RANDOMKEY(self):
        """Compatible, but slow: O(n)."""
        if self.ht:
            return random.choice(self.ht.keys())

    def RENAME(self, key, newkey):
        """Fully compatible."""
        assert key != newkey
        self.ht[newkey] = self.ht[key]
        del self.ht[key]
        return OK

    def RENAMENX(self, key, newkey):
        """Fully compatible."""
        assert key != newkey
        assert key in self.ht
        if newkey in self.ht:
            return 0
        else:
            self.ht[newkey] = self.ht[key]
            del self.ht[key]
            return 1

    def RESTORE(self, key, ttl, serialized_value):
        """Non-standard; uses Pickle instead of Redis format."""
        if ttl != '0':
            raise NotImplementedError
        self.ht[key] = pickle.loads(serialized_value)
        return OK

    def SORT(self, key, *args):
        """Mostly compatible."""
        # default values
        pattern = None
        offset = 0
        count = None
        patterns = []
        reverse = False
        alpha = False
        store = None
        # parse args
        curr_arg = lambda: args[0].upper()
        next_arg = lambda: args.pop(0)
        if curr_arg() == 'BY':
            next_arg()
            pattern = next_arg()
        if curr_arg() == 'LIMIT':
            next_arg()
            offset = int(next_arg())
            count = int(next_arg())
        while curr_arg() == 'GET':
            next_arg()
            patterns.append(next_arg())
        if curr_arg() == 'ASC':
            next_arg()
        elif curr_arg() == 'DESC':
            next_arg()
            reverse = True
        if curr_arg() == 'ALPHA':
            next_arg()
            alpha = True
        if curr_arg() == 'STORE':
            next_arg()
            store = next_arg()
        assert not args
        # to be continued...
        raise NotImplementedError

    def TTL(self, key):
        raise NotImplementedError

    _typemap = {
        str: 'string',
        deque: 'list',
        set: 'set',
        dict: 'hash',
        type(None): 'none',
    }

    def TYPE(self, key):
        """Fully compatible."""
        return self._typemap[type(self.ht.get(key, None))]

    # Strings

    def APPEND(self, key, value):
        """Fully compatible."""
        old_value = self.ht.get(key, '')
        assert isinstance(old_value, str)
        self.ht[key] = old_value + value
        return len(self.ht[key])

    def BITCOUNT(self, key):
        raise NotImplementedError

    def BITOP(self, operation, destkey, *keys):
        raise NotImplementedError

    def DECR(self, key):
        """Fully compatible."""
        return self.INCRBY(key, '-1')

    def DECRBY(self, key, decrement):
        """Fully compatible."""
        return self.INCRBY(key, -int(decrement))

    def GET(self, key):
        """Fully compatible."""
        value = self.ht.get(key)
        if isinstance(value, str):
            return value
        elif value is None:
            return None
        else:
            raise ValueError('wrong type for GET')

    def GETBIT(self, key, offset):
        raise NotImplementedError

    def GETRANGE(self, key, start, end):
        """Fully compatible."""
        start = int(start)
        end = int(end)
        value = self.ht.get(key, '')
        return value[redis_slice(start, end)]

    def GETSET(self, key, value):
        """Fully compatible."""
        old_value = self.ht.get(key, '')
        assert isinstance(value, str)
        self.ht[key] = value
        return old_value

    def INCR(self, key):
        """Fully compatible."""
        return self.INCRBY(key, '1')

    def INCRBY(self, key, increment):
        """Fully compatible."""
        value = self.ht.get(key, '0')
        assert isinstance(value, str)
        self.ht[key] = str(int(value) + int(increment))
        return self.ht[key]

    def INCRBYFLOAT(self, key, increment):
        """Fully compatible."""
        value = self.ht.get(key, '0')
        assert isinstance(value, str)
        result = '%.17f' % (float(value) + float(increment))
        self.ht[key] = result.rstrip('0').rstrip('.')
        return self.ht[key]
 
    def MGET(self, *keys):
        """Fully compatible."""
        assert keys
        values = []
        for key in keys:
            value = self.ht.get(key, '')
            if isinstance(value, str):
                values.append(value)
            else:
                values.append(None)
        return values

    def MSET(self, *args):
        """Fully compatible."""
        assert args
        assert len(args) % 2 == 0
        for index in xrange(0, len(args), 2):
            key = args[index]
            value = args[index+1]
            self.ht[key] = value
        return OK

    def MSETNX(self, *args):
        """Fully compatible."""
        assert args
        assert len(args) % 2 == 0
        for index in xrange(0, len(args), 2):
            key = args[index]
            if key in self.ht:
                return 0
        for index in xrange(0, len(args), 2):
            key = args[index]
            value = args[index+1]
            self.ht[key] = value
        return 1

    def PSETEX(self, key, milliseconds, value):
        raise NotImplementedError

    def SET(self, key, value):
        """Fully compatible."""
        self.ht[key] = value
        return OK

    def SETBIT(self, key, offset, value):
        raise NotImplementedError

    def SETEX(self, key, seconds, value):
        raise NotImplementedError

    def SETNX(self, key, value):
        """Fully compatible."""
        if key not in self.ht:
            self.ht[key] = value
            return 1
        else:
            return 0

    def SETRANGE(self, key, offset, value):
        """Fully compatible."""
        offset = int(offset)
        old_value = self.ht.get(key, '')
        assert isinstance(old_value, str)
        self.ht[key] = old_value[:offset] + value + old_value[offset+len(value)]
        return len(self.ht[key])

    def STRLEN(self, key):
        """Fully compatible."""
        value = self.ht.get(key, '')
        assert isinstance(value, str)
        return len(value)

    # Hashes

    @hashmethod
    def HDEL(self, hash, *fields):
        """Fully compatible."""
        deleted = 0
        for field in fields:
            if field in hash:
                del hash[field]
        return deleted

    @hashmethod
    def HEXISTS(self, hash, field):
        """Fully compatible."""
        if field in hash:
            return 1
        else:
            return 0

    @hashmethod
    def HGET(self, hash, field):
        """Fully compatible."""
        return hash.get(field)

    @hashmethod
    def HGETALL(self, hash):
        """Fully compatible."""
        result = []
        for field, value in hash:
            result.append(field)
            result.append(value)
        return result

    @hashmethod
    def HINCRBY(self, hash, field, increment):
        """Fully compatible."""
        hash[field] = str(int(hash.get(field, 0)) + int(increment))
        return hash[field]


    @hashmethod
    def HINCRBYFLOAT(self, hash, field, increment):
        """Fully compatible."""
        result = '%.17f' % (float(hash.get(field, 0)) + float(increment))
        hash[field] = result.rstrip('0').rstrip('.')
        return hash[field]

    @hashmethod
    def HKEYS(self, hash):
        """Fully compatible."""
        return hash.keys()

    @hashmethod
    def HLEN(self, hash):
        """Fully compatible."""
        return len(hash)

    @hashmethod
    def HMGET(self, hash, *fields):
        """Fully compatible."""
        assert fields
        return map(hash.get, fields)

    @hashmethod
    def HMSET(self, hash, *args):
        """Fully compatible."""
        assert args
        assert len(args) % 2 == 0
        for field, value in args:
            hash[field ] = value
        return OK

    @hashmethod
    def HSET(self, hash, field, value):
        """Fully compatible."""
        if field not in hash:
            hash[field] = value
            return 1
        else:
            hash[field] = value
            return 0

    @hashmethod
    def HSETNX(self, hash, field, value):
        """Fully compatible."""
        if field not in hash:
            hash[field] = value
            return 1
        else:
            return 0

    @hashmethod
    def HVALS(self, hash):
        """Fully compatible."""
        return hash.values()

    # Lists

    @listmethod
    def LINDEX(self, list, index):
        """Fully compatible."""
        return list[int(index)]

    @listmethod
    def LINSERT(self, list, where, pivot, value):
        raise NotImplementedError

    @listmethod
    def LLEN(self, list):
        """Fully compatible."""
        return len(list)

    @listmethod
    def LPOP(self, list):
        try:
            return list.popleft()
        except IndexError:
            return None

    @listmethod
    def LPUSH(self, list, *values):
        assert values
        list.extendleft(values)
        return len(list)

    def LPUSHX(self, key, value):
        raise NotImplementedError

    @listmethod
    def LRANGE(self, list, start, stop):
        return list[redis_slice(start, stop)]

    @listmethod
    def LREM(self, list, count, value):
        raise NotImplementedError

    @listmethod
    def LSET(self, list, index, value):
        list[int(index)] = value
        return OK

    @listmethod
    def LTRIM(self, list, start, stop):
        raise NotImplementedError

    @listmethod
    def RPOP(self, list):
        try:
            return list.pop()
        except IndexError:
            return None

    @listmethod
    def RPUSH(self, list, *values):
        assert values
        list.extend(values)
        return len(list)

    def RPUSHX(self, key, value):
        raise NotImplementedError

    # Sets

    @setmethod
    def SADD(self, set, *members):
        assert members
        added = 0
        for member in members:
            if member not in set:
                set.add(member)
                added += 1
        return added

    @setmethod
    def SCARD(self, set):
        return len(set)

    @setmethod
    def SMEMBERS(self, set):
        return list(set)

    @setmethod
    def SDIFF(self, set, *sets):
        raise NotImplementedError

    def SDIFFSTORE(self, destination, *keys):
        raise NotImplementedError

    @setmethod
    def SPOP(self, set):
        if set:
            return set.pop()
        else:
            return None

    @setmethod
    def SRANDMEMBER(self, set):
        if set:
            member = set.pop()
            set.add(member)
            return member
        else:
            return None

    @setmethod
    def SREM(self, set, *members):
        assert members
        removed = 0
        for member in members:
            if member in set:
                set.remove(member)
                removed += 1
        return removed


    # Connection

    def AUTH(self, password):
        return NotImplementedError

    def ECHO(self, message):
        """Fully compatible."""
        return message

    def PING(self):
        """Fully compatible."""
        return Status('PONG')

    def QUIT(self):
        raise NotImplementedError

    def SELECT(self, db):
        """Fully compatible."""
        self.ht = self.dbs[int(db)]
        return OK

    # Server

    def DBSIZE(self):
        """Fully compatible."""
        return len(self.ht)

    def DEBUG(self, subcommand, *args):
        """Non-standard, partially implemented."""
        subcommand = subcommand.upper()
        if subcommand == 'SEGFAULT':
            # why would you do this?
            assert not args
            os.kill(os.getpid(), signal.SIGSEGV)
            return OK
        elif subcommand == 'HT':
            return str(self.ht)
        else:
            raise ValueError

    def FLUSHALL(self):
        """Fully compatible."""
        for db in self.dbs:
            db.clear()
        return OK

    def FLUSHDB(self):
        """Fully compatible."""
        self.ht.clear()
        return OK

    def INFO(self):
        """Non-standard (implementation-specific command)."""
        sysname, nodename, release, version, machine = os.uname()
        lines = [
            'server:cardboard-redis',
            'os:%s %s %s' % (sysname, release, machine),
            'python:%s.%s.%s' % sys.version_info[0:3],
        ]
        for dbid, db in enumerate(self.dbs):
            if len(db) > 0:
                lines.append('db%d:keys=%d' % (dbid, len(db)))
        return ''.join([line+'\r\n' for line in lines])

    def TIME(self):
        """Fully compatible."""
        now = time.time()
        usecs, secs = math.modf(now)
        return [str(int(1000000.0*usecs)), str(int(secs))]
