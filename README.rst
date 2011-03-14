Tako
====
Tako is a distributed key-value data store. It aims to provide high scalability and availability through a shared nothing architecture, consistent hash based data partitioning, read repair with time stamping and live migration. An included coordinator server can be used as a single point of configuration to distribute cluster configuration to tako nodes in a cluster.

Tako includes a http proxy server that can be used to interface with a tako cluster using normal HTTP GET/POST.

Within a cluster, tako nodes communicate using a binary protocol and the socketless and syncless libraries.

Tako makes use of libev/libevent/kqueue/kpoll if present.
Tokyo Cabinet is used for data storage.

Project Home: http://pypi.python.org/pypi/tako

Installation
============

Base
----

These instructions are written for Debian Squeeze (Stable).
Lenny might work as well but you might hit some snags with Python 2.5.

It is recommended that Tako is installed using virtualenv.

First, some prerequisites:

    $ apt-get install build-essential python-virtualenv python-dev libev-dev libtokyocabinet-dev

Next, the virtual environment that will host the tako installation.

    $ virtualenv tako

    $ cd tako

    $ bin/pip install tako


Node
----

Coordinator
-----------

Proxy
-----

