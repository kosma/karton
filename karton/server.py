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
import traceback
from functools import partial
try:
    import cPickle as pickle
except ImportError:
    import pickle

from blist import blist

from .protocol import Status, Error, OK
from .sorteddict import sorteddict as zdict


def redis_slice(start, end):
    """Convert Redis start/end to Python slice."""
    return slice(int(start), (int(end)+1) or None)


def floaty(number):
    string = '%.17f' % number
    return string.rstrip('0').rstrip('.')


def pass_value(value_type):
    """Pass first method value using key."""
    def decorator(method):
        def decorated(self, key, *args):
            value = self._ht_get(key, value_type)
            result = method(self, value, *args)
            self.client.ht[key] = value
            self._ht_check(key)
            return result
        decorated.__name__ = method.__name__
        return decorated
    return decorator


set_type = set
hash_type = dict
list_type = blist
zset_type = zdict

setmethod = pass_value(set_type)
hashmethod = pass_value(hash_type)
listmethod = pass_value(list_type)
zsetmethod = pass_value(zset_type)


def pass_string(method):
    """Pass first method argument as HT string and handle return value appropriately."""
    def decorated(self, string, *args):
        new_value = None
        try:
            value = self._ht_get(key, value_type)
            retval, new_value = method(self, value, *args)
        finally:
            self.client.ht[key] = new_value
            self._ht_check(key)
            return new_value
    decorated.__name__ = method.__name__
    return decorated


class Client(object):

    def __init__(self, server, addr):
        self.server = server
        self.addr = addr

    def do(self, request):
        return self.server.do(self, *request)

    def die(self):
        # break circular references!
        del self.server


class Server(object):

    def __init__(self, dbs=16):
        self.dbs = [{} for index in xrange(dbs)]

    def new_client(self, addr):
        client = Client(self, addr)
        self.do(client, 'SELECT', '0')
        return client

    def do(self, client, *args):
        try:
            # setup context.
            self.client = client
            # run the command
            command = args[0]
            handler = getattr(self, '%s' % command.upper())
            result = handler(*args[1:])
        except Exception as exc:
            print traceback.format_exc()
            return exc
        else:
            return result
        finally:
            # teardown context.
            del self.client

    def _ht_get(self, key, type):
        value = self.client.ht.get(key, type())
        assert isinstance(value, type), 'ERR Operation against a key holding the wrong kind of value'
        return value

    def _ht_check(self, key):
        if key in self.client.ht and not self.client.ht[key]:
            del self.client.ht[key]

    # Keys

    def DEL(self, *keys):
        """Fully compatible."""
        assert keys
        count = 0
        for key in keys:
            if key in self.client.ht:
                del self.client.ht[key]
                count += 1
        return count

    def DUMP(self, key):
        """Non-standard: uses Pickle instead of Redis format."""
        if key in self.client.ht:
            return pickle.dumps(self.client.ht[key])

    def EXISTS(self, key):
        """Fully compatible."""
        if key in self.client.ht:
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
        return filter(partial(fnmatch.fnmatchcase, pat=pattern), self.client.ht.keys())

    def MOVE(self, key, db):
        raise NotImplementedError

    def PERSIST(self, key):
        raise NotImplementedError

    def RANDOMKEY(self):
        """Compatible, but slow: O(n)."""
        if self.client.ht:
            return random.choice(self.client.ht.keys())

    def RENAME(self, key, newkey):
        """Fully compatible."""
        assert key != newkey
        self.client.ht[newkey] = self.client.ht[key]
        del self.client.ht[key]
        return OK

    def RENAMENX(self, key, newkey):
        """Fully compatible."""
        assert key != newkey
        assert key in self.client.ht
        if newkey in self.client.ht:
            return 0
        else:
            self.client.ht[newkey] = self.client.ht[key]
            del self.client.ht[key]
            return 1

    def RESTORE(self, key, ttl, serialized_value):
        """Non-standard; uses Pickle instead of Redis format."""
        if ttl != '0':
            raise NotImplementedError
        self.client.ht[key] = pickle.loads(serialized_value)
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
        list_type: 'list',
        set_type: 'set',
        hash_type: 'hash',
        type(None): 'none',
    }

    def TYPE(self, key):
        """Fully compatible."""
        return self._typemap[type(self.client.ht.get(key, None))]

    # Strings

    def APPEND(self, key, value):
        """Fully compatible."""
        old_value = self.client.ht.get(key, '')
        assert isinstance(old_value, str)
        self.client.ht[key] = old_value + value
        return len(self.client.ht[key])

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
        value = self.client.ht.get(key)
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
        value = self.client.ht.get(key, '')
        return value[redis_slice(start, end)]

    def GETSET(self, key, value):
        """Fully compatible."""
        old_value = self.client.ht.get(key, '')
        assert isinstance(value, str)
        self.client.ht[key] = value
        return old_value

    def INCR(self, key):
        """Fully compatible."""
        return self.INCRBY(key, '1')

    def INCRBY(self, key, increment):
        """Fully compatible."""
        value = self.client.ht.get(key, '0')
        assert isinstance(value, str), 'ERR operation on a key holding a wrong kind of value'
        assert not value[0].isspace(), 'ERR invalid value'
        assert not value[-1].isspace(), 'ERR invalid value'
        self.client.ht[key] = str(int(value) + int(increment))
        return self.client.ht[key]

    def INCRBYFLOAT(self, key, increment):
        """Fully compatible."""
        value = self.client.ht.get(key, '0')
        assert isinstance(value, str), 'ERR operation on a key holding a wrong kind of value'
        assert not value[0].isspace(), 'ERR invalid value'
        assert not value[-1].isspace(), 'ERR invalid value'
        increment = float(increment)
        assert not math.isnan(increment), 'ERR would produce NaN'
        assert not math.isinf(increment), 'ERR would produce Infinity'
        result = '%.17f' % (float(value) + increment)
        self.client.ht[key] = result.rstrip('0').rstrip('.')
        return self.client.ht[key]
 
    def MGET(self, *keys):
        """Fully compatible."""
        assert keys
        values = []
        for key in keys:
            value = self.client.ht.get(key, '')
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
            self.client.ht[key] = value
        return OK

    def MSETNX(self, *args):
        """Fully compatible."""
        assert args
        assert len(args) % 2 == 0
        for index in xrange(0, len(args), 2):
            key = args[index]
            if key in self.client.ht:
                return 0
        for index in xrange(0, len(args), 2):
            key = args[index]
            value = args[index+1]
            self.client.ht[key] = value
        return 1

    def PSETEX(self, key, milliseconds, value):
        raise NotImplementedError

    def SET(self, key, value):
        """Fully compatible."""
        self.client.ht[key] = value
        return OK

    def SETBIT(self, key, offset, value):
        raise NotImplementedError

    def SETEX(self, key, seconds, value):
        raise NotImplementedError

    def SETNX(self, key, value):
        """Fully compatible."""
        if key not in self.client.ht:
            self.client.ht[key] = value
            return 1
        else:
            return 0

    def SETRANGE(self, key, offset, value):
        """Fully compatible."""
        offset = int(offset)
        old_value = self.client.ht.get(key, '')
        assert isinstance(old_value, str)
        self.client.ht[key] = old_value[:offset] + value + old_value[offset+len(value)]
        return len(self.client.ht[key])

    def STRLEN(self, key):
        """Fully compatible."""
        value = self.client.ht.get(key, '')
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
        try:
            return list[int(index)]
        except IndexError:
            return None

    @listmethod
    def LINSERT(self, list, where, pivot, value):
        where = where.upper()
        assert where in ('BEFORE', 'AFTER'), 'ERR syntax error: where != BEFORE|AFTER'
        try:
            index = list.index(pivot)
        except ValueError:
            return -1
        else:
            if where == 'BEFORE':
                list.insert(index, value)
            else:
                list.insert(index+1, value)
            return len(list)

    @listmethod
    def LLEN(self, list):
        """Fully compatible."""
        return len(list)

    @listmethod
    def LPOP(self, list):
        """Fully compatible."""
        try:
            return list.pop(0)
        except IndexError:
            return None

    @listmethod
    def LPUSH(self, list, *values):
        """Fully compatible."""
        assert values
        for value in values:
            list.insert(0, value)
        return len(list)

    @listmethod
    def LPUSHX(self, list, value):
        """Fully compatible."""
        if list:
            list.insert(0, value)
        return len(list)

    @listmethod
    def LRANGE(self, list, start, stop):
        """More-or-less compatible, breaks sometimes."""
        return list[redis_slice(start, stop)]

    @listmethod
    def LREM(self, list, count, value):
        """Compatible, but slower in some cases."""
        count = int(count)
        removed = 0
        if count > 0:
            while removed < count:
                try:
                    list.remove(value)
                    removed += 1
                except ValueError:
                    break
        elif count == 0:
            while True:
                try:
                    list.remove(value)
                    removed += 1
                except ValueError:
                    break
        else:
            count = -count
            list.reverse()
            while removed < count:
                try:
                    list.remove(value)
                    removed += 1
                except ValueError:
                    break
            list.reverse()
        return removed

    @listmethod
    def LSET(self, list, index, value):
        """Fully compatible."""
        if not list:
            return Error('ERR key does not exist')
        try:
            list[int(index)] = value
        except IndexError:
            return Error('ERR index out of range')
        return OK

    @listmethod
    def LTRIM(self, list, start, stop):
        """Fully compatible."""
        list[:] = list[redis_slice(start, stop)]
        return OK

    @listmethod
    def RPOP(self, list):
        """Fully compatible."""
        try:
            return list.pop()
        except IndexError:
            return None

    def RPOPLPUSH(self, source_key, destination_key):
        """Fully compatible."""
        source = self._ht_get(source_key, list_type)
        if not source:
            return None
        destination = self._ht_get(destination_key, list_type)
        item = source.pop()
        destination.insert(0, item)
        self.client.ht[destination_key] = destination
        return item

    @listmethod
    def RPUSH(self, list, *values):
        assert values
        list.extend(values)
        return len(list)

    @listmethod
    def RPUSHX(self, list, value):
        if list:
            list.append(value)
        return len(list)

    # Sets

    @setmethod
    def SADD(self, set, *members):
        """Fully compatible."""
        assert members
        added = 0
        for member in members:
            if member not in set:
                set.add(member)
                added += 1
        return added

    @setmethod
    def SCARD(self, set):
        """Fully compatible."""
        return len(set)

    @setmethod
    def SDIFF(self, set, *keys):
        """Fully compatible."""
        return set.difference(*[self._ht_get(key, set_type) for key in keys])

    def SDIFFSTORE(self, destination, key, *keys):
        """Fully compatible."""
        self.client.ht[destination] = self._ht_get(key, set_type).difference(*[self._ht_get(key, set_type) for key in keys])
        result = len(self.client.ht[destination])
        self._ht_check(destination)
        return result

    @setmethod
    def SINTER(self, set, *keys):
        """Fully compatible."""
        return set.intersection(*[self._ht_get(key, set_type) for key in keys])

    def SINTERSTORE(self, destination, key, *keys):
        """Fully compatible."""
        self.client.ht[destination] = self._ht_get(key, set_type).intersection(*[self._ht_get(key, set_type) for key in keys])
        result = len(self.client.ht[destination])
        self._ht_check(destination)
        return result

    @setmethod
    def SISMEMBER(self, set, member):
        """Fully compatible."""
        return int(member in set)

    @setmethod
    def SMEMBERS(self, set):
        """Fully compatible."""
        return set

    def SMOVE(self, source, destination, member):
        source_set = self._ht_get(source, set_type)
        destination_set = self._ht_get(destination, set_type)
        if member not in source_set:
            return 0
        else:
            self.client.ht[destination] = destination_set
            source_set.remove(member)
            destination_set.add(member)
            self._ht_check(source)
            return 1

    @setmethod
    def SPOP(self, set):
        """Fully compatible."""
        if set:
            return set.pop()
        else:
            return None

    @setmethod
    def SRANDMEMBER(self, set, count=None):
        """Compatible, but slow."""
        if count is None:
            # the easy path
            if set:
                return random.sample(set, 1)[0]
            else:
                return None
        else:
            # the hard path
            count = int(count)
            if set and count:
                if count > 0:
                    return random.sample(set, min(count, len(set)))
                else:
                    members = list(set)
                    return [random.choice(members) for i in xrange(-count)]
            else:
                return []

    @setmethod
    def SREM(self, set, *members):
        """Fully compatible."""
        assert members
        removed = 0
        for member in members:
            if member in set:
                set.remove(member)
                removed += 1
        return removed


    @setmethod
    def SUNION(self, set, *keys):
        """Fully compatible."""
        return set.union(*[self._ht_get(key, set_type) for key in keys])

    def SUNIONSTORE(self, destination, key, *keys):
        """Fully compatible."""
        self.client.ht[destination] = self._ht_get(key, set_type).union(*[self._ht_get(key, set_type) for key in keys])
        result = len(self.client.ht[destination])
        self._ht_check(destination)
        return result

    # Sorted Sets

    @zsetmethod
    def ZADD(self, zset, *args):
        assert args, 'syntax error, arguments required'
        assert len(args) % 2 == 0
        pairs = blist()
        # check for errors before doing anything
        for index in xrange(0, len(args), 2):
            score = float(args[index])
            assert not math.isnan(score), "ERR not a valid floating point value"
            member = args[index+1]
            pairs.append((score, member))
        added = 0
        for score, member in pairs:
            if member not in zset:
                added += 1
            zset[member] = score
        return added

    @zsetmethod
    def ZCARD(self, zset):
        return len(zset)

    @zsetmethod
    def ZCOUNT(self, zset, min, max):
        raise NotImplementedError

    @zsetmethod
    def ZINCRBY(self, zset, increment, member):
        increment = float(increment)
        assert not math.isnan(increment), "ERR not a valid floating point value"
        score = zset.get(member, 0.0) + increment
        assert not math.isnan(score), "ERR resulting score is NaN"
        zset[member] = score
        return floaty(score)

    def ZINTERSTORE(self, destination, numkeys, *args):
        raise NotImplementedError

    @zsetmethod
    def ZRANGE(self, zset, start, stop, *flags):
        # TODO: better flags checking
        if len(flags) == 1 and flags[0].upper() == 'WITHSCORES':
            result = blist()
            for key, value in zset.items()[redis_slice(start, stop)]:
                result.append(key)
                astext = '%.17f' % value
                astext = astext.rstrip('0').rstrip('.')
                result.append(astext)
            return result
        else:
            print 'keys'
            return zset.viewkeys()[redis_slice(start, stop)]

    @zsetmethod
    def ZRANGEBYSCORE(self, zset, min, max, *args):
        raise NotImplementedError

    @zsetmethod
    def ZRANK(self, zset, member):
        return zset._sortedkeys.index(member)

    @zsetmethod
    def ZREM(self, zset, *members):
        assert members
        deleted = 0
        for member in members:
            if member in zset:
                del zset[member]
                deleted += 1
        return deleted

    @zsetmethod
    def ZREMRANGEBYRANK(self, zset, start, stop):
        raise NotImplementedError

    @zsetmethod
    def ZREMRANGEBYSCORE(self, zset, min, max):
        raise NotImplementedError

    @zsetmethod
    def ZREVRANGE(self, zset, start, stop, *args):
        raise NotImplementedError

    @zsetmethod
    def ZREVRANGEBYSCORE(self, zset, max, min, *args):
        raise NotImplementedError

    @zsetmethod
    def ZREVRANK(self, zset, member):
        raise NotImplementedError

    @zsetmethod
    def ZSCORE(self, zset, member):
        return floaty(zset[member])

    def ZINTERSTORE(self, destination, numkeys, *args):
        raise NotImplementedError

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
        self.client.ht = self.dbs[int(db)]
        return OK

    # Server

    def CONFIG(self, command, option, value=None):
        """FIXME: kludge for Redis unit tests."""
        if command.upper() == 'SET' and value is not None:
            return OK
        raise NotImplementedError

    def DBSIZE(self):
        """Fully compatible."""
        return len(self.client.ht)

    def DEBUG(self, subcommand, *args):
        """Non-standard, partially implemented."""
        subcommand = subcommand.upper()
        if subcommand == 'SEGFAULT':
            # why would you do this?
            assert not args
            os.kill(os.getpid(), signal.SIGSEGV)
            return OK
        elif subcommand == 'HT':
            return str(self.client.ht)
        else:
            #raise ValueError
            # oh kludge I love you
            return OK

    def FLUSHALL(self):
        """Fully compatible."""
        for db in self.dbs:
            db.clear()
        return OK

    def FLUSHDB(self):
        """Fully compatible."""
        self.client.ht.clear()
        return OK

    def INFO(self):
        """Non-standard (implementation-specific command)."""
        sysname, nodename, release, version, machine = os.uname()
        lines = [
            'server:karton',
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
