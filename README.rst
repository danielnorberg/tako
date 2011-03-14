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

This describes the process of installing the Tako software on a single machine.
In a typical setup this setup would be replicated/performed on all the machines that are to form the cluster.

These instructions are written for Debian Squeeze (Stable).
Lenny might work as well but you might hit some snags with Python 2.5.

It is recommended that Tako is installed using virtualenv.

Base
----

First, some prerequisites:

::

    $ apt-get install build-essential python-virtualenv python-dev libev-dev libtokyocabinet-dev

Next, the virtual environment that will host the tako installation.
(Note: A default setup of Tako will need write permissions to this directory.)

::

    $ virtualenv tako

Lastly, install the tako module and its dependencies.

::

    $ cd tako
    $ bin/pip install tako

Now continue on to Node, Coordinator or Proxy.

Node
----

Done! Start tako-node:

::

    $ bin/tako-node -id <id of the node> -c <address and port of the coordinator server>

Coordinator
-----------

Setup a cluster configuration:

::

    $ mkdir etc
    $ wget --no-check-certificate https://github.com/danielnorberg/tako/raw/master/examples/cluster.yaml -O etc/tako.yaml

Modify configuration file as needed.

Done! Start tako-coordinator:

::

    $ bin/tako-coordinator -cfg etc/tako.yaml

Proxy
-----

Done! Start tako-proxy:

::

    $ bin/tako-proxy -p <http port> -id <id of the proxy> -c <address and port of the coordinator server>

Executables
===========

tako-node
---------

tako-coordinator
----------------

tako-proxy
----------


Sample Configuration Files
==========================

standalone.yaml
---------------

This configuration sets up a single stand-alone node.

::

    # Tako Configuration
    ---
    active_deployment: standalone
    deployments:
        standalone:
            read_repair: no
            hash:
                buckets_per_key: 1
            buckets:
                b1:
                    n1: [localhost, 5711, 4711]

cluster.yaml
------------

This configuration sets up 10 nodes in 5 buckets, 2 nodes per bucket.
The replication factor buckets_per_key is set to 2 which causes every
key-value pair to be replicated across 2 buckets with 2 nodes for a total
of 4 nodes. Read repair is enabled.

A single coordinator serves the below configuration to the node cluster.

::

    # Tako Configuration
    #
    # NOTE: The contents of this file may be json-serialized. For dictionary keys, only use strings.
    ---
    master_coordinator: c1

    coordinators:
        c1: [tako-coordinator-1.domain.com, 4712]

    active_deployment: cluster

    deployments:
        cluster:
            read_repair: yes
            hash:
                buckets_per_key: 2
            buckets:
                b1:
                    n1:  [tako-node-01.domain.com, 5711, 4711]
                    n2:  [tako-node-02.domain.com, 5711, 4711]
                b2:
                    n3:  [tako-node-03.domain.com, 5711, 4711]
                    n4:  [tako-node-04.domain.com, 5711, 4711]
                b3:
                    n5:  [tako-node-05.domain.com, 5711, 4711]
                    n6:  [tako-node-06.domain.com, 5711, 4711]
                b4:
                    n7:  [tako-node-07.domain.com, 5711, 4711]
                    n8:  [tako-node-08.domain.com, 5711, 4711]
                b5:
                    n9:  [tako-node-09.domain.com, 5711, 4711]
                    n10: [tako-node-10.domain.com, 5711, 4711]

