cardboard
=========

Redis reimplementation in Python.

Usage
-----

NOTE: **This project is work in progress.** Things *will* break.

**Standalone**: start with ``twisted_cardboard.py``. Use your regular client or
simply ``redis-cli`` to interact with it.

**Embedded**: work in progress, as the interface is not stable yet.

Caveats
-------

* There is no persistence. When the instance dies, you lose all the data.
* Not all commands are implemented.
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

There is a Python project named cardboard already!
--------------------------------------------------

I know. This is a placeholder codename which will change once I invent
something better.
