karton
======

Redis reimplementation in Python.

*"Karton"* means cardboard in Polish, and reflects the author's tendency to build
stuff out of whatever is at hand, even if it doesn't turn out as sturdy as
desired.

NOTE: This project is work in progress.

Usage
-----

Requirements: ``twisted``, ``hiredis``, ``blist``.

Clone the repository. Start with ``./twisted_karton.py``. Use your favourite
client or simply ``redis-cli`` to interact with it.

Status
------

* Starts. (yay!)
* Basic commands work.
* You can actually run the Redis test suite against it! ``./run_redis_tests``
* Test suites passing so far: ``unit/type/set``, ``unit/type/list`` (except
  blocking commands they're not supported yet - but should be easy enough
  to implement using Twisted's Deferreds).

Caveats
-------

* This project hasn't even reached the proof-of-concept level yet.
* There is no persistence. When the instance dies, you lose all the data.
* Not all commands are implemented (e.g. no expires, no zsets).
* There are dozens of unfixed bugs.

Goals
-----

* Full data command compatibility.
* Master mode.
* Slave mode.
* Lua scripting.
* Drop-in compability.
* No user annoyance. I don't want to create a product which implements 99%
  of the original spec but has dumbass caveats like "you can't have binary data
  as zset values" or "you can't use SELECT in MULTI".

Non-goals
---------

* Mindless 1:1 compabibility. Don't expect DUMP or SLOWLOG to return the same
  data as the original Redis. Don't expect the log output or configuration file
  format to be identical. If I wanted Redis experience, I'd just use Redis.

Why would you do this?
----------------------

Because it's fun. Because I can. Because having an embeddable, zero-latency,
synchronous Redis instance *inside* my Python program is simply awesome.
