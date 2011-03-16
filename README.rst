Tako
====
Tako is a distributed key-value data store. It aims to provide high scalability and availability through a shared nothing architecture, consistent hash based data partitioning, read repair with time stamping and live migration. An included coordinator server can be used as a single point of configuration to distribute cluster configuration to tako nodes in a cluster.

Tako includes a http proxy server that can be used to interface with a tako cluster using normal HTTP GET/POST.

Within a cluster, tako nodes communicate using a binary protocol and the socketless and syncless libraries.

Tako makes use of libev/libevent/kqueue/kpoll if present.
Tokyo Cabinet is used for data storage.

Project Home: http://pypi.python.org/pypi/tako


Getting Started
===============

This describes the process of installing the Tako software on a single machine.
In a typical setup this setup would be replicated/performed on all the machines that are to form the cluster.

These instructions are written for Debian Squeeze (Stable).
Lenny might work as well but you might hit some snags with Python 2.5.

It is recommended that Tako is installed using virtualenv.

Installation
------------

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

This concludes the installation. You can now run a local test cluster using ``tako-cluster`` or use ``tako-node``, ``tako-coordinator`` and ``tako-proxy`` to run a customized setup. For a a test run walkthrough, continue with *Test Run* below.


Test Run
========

There's lots of data sets out there that can be used to experiment with a tako cluster, but I like music so I'm going to use the million song dataset subset. If you have a lot of time and diskspace and/or machines, try out the full data set and let me know about it =)

First, download the million song subset. The infochimps mirror might be faster.

    http://labrosa.ee.columbia.edu/millionsong/

    http://www.infochimps.com/datasets/the-million-song-dataset-10k-songs-subset

Using tako-cluster we can quickly get a tako cluster up and running on a single machine. I'll use the local_cluster.yaml with a proxy on port 8080.

::

    # Download configuration file
    mkdir etc
    wget --no-check-certificate https://github.com/danielnorberg/tako/raw/master/examples/local_cluster.yaml -O etc/local_tako.yaml

    # Start the local tako cluster
    tako/bin/tako-cluster tako/etc/local_cluster.yaml -p 8080

Now we'll populate the tako cluster using the dataset and then pull it back out again. If you're running a different tako cluster setup, simply adjust the proxy address and port below.

::

    # Unpack the dataset
    tar xz millionsongsubset.tar.gz

    # Upload the dataset into the tako cluster using wget and a tako proxy
    for f in `find MillionSongSubset -name '*.h5'`; do wget -nv -O /dev/null --post-file=$f http://localhost:8080/values/$(basename $f); done

    # Download the dataset again...
    mkdir fetched
    for f in `find MillionSongSubset -name '*.h5'`; do wget -P fetched -nv http://localhost:8080/values/$(basename $f); done

    # ...and compare all the files, making sure that they survived the roundtrip intact.
    for f in `find MillionSongSubset -name '*.h5'`; do if cmp $f fetched/$(basename $f); then echo $f: Identical; else echo $f: Differing; fi done

Sample Configuration Files
==========================

standalone.yaml
---------------

This configuration sets up a single stand-alone node. Read repair and background healing is not possible in this setup and are thus disabled.

::

    # Tako Configuration
    # standalone.yaml
    ---
    active_deployment: standalone
    deployments:
        standalone:
            read_repair: no
            background_healing: no
            hash:
                buckets_per_key: 1
            buckets:
                b1:
                    n1: [localhost, 5711, 4711]

cluster.yaml
------------

This configuration sets up 10 nodes in 5 buckets, 2 nodes per bucket.
The replication factor ``buckets_per_key`` is set to 2 which causes every
key-value pair to be replicated across 2 buckets with 2 nodes for a total
of 4 nodes.

Both read repair and background healing is enabled, with the background healing scheduled to be performed at 24 hour intervals. Larger data sets typically need larger intervals, otherwise the background healing will take up too much resources simply to go through all the key/value pairs and communicate with peers.

A single coordinator serves the below configuration to the node cluster.

::

    # Tako Configuration
    # cluster.yaml
    # NOTE: The contents of this file may be json-serialized. For dictionary keys, only use strings.
    ---
    master_coordinator: c1
    coordinators:
        c1: [tako-coordinator-1.domain, 4710]

    active_deployment: cluster

    deployments:
        cluster:
            read_repair: yes
            background_healing: yes
            background_healing_interval: 1d 0:00:00
            hash:
                buckets_per_key: 2
            buckets:
                b1:
                    n1:  [tako-node-01.domain, 5711, 4711]
                    n2:  [tako-node-02.domain, 5711, 4711]
                b2:
                    n3:  [tako-node-03.domain, 5711, 4711]
                    n4:  [tako-node-04.domain, 5711, 4711]
                b3:
                    n5:  [tako-node-05.domain, 5711, 4711]
                    n6:  [tako-node-06.domain, 5711, 4711]
                b4:
                    n7:  [tako-node-07.domain, 5711, 4711]
                    n8:  [tako-node-08.domain, 5711, 4711]
                b5:
                    n9:  [tako-node-09.domain, 5711, 4711]
                    n10: [tako-node-10.domain, 5711, 4711]

local_cluster.yaml
------------------

Like ``cluster.yaml`` but written to run locally on a single machine using ``tako-cluster``. Note that every node uses different ports.

::

    # Tako Configuration
    # local_cluster.yaml
    # NOTE: The contents of this file may be json-serialized. For dictionary keys, only use strings.
    ---
    master_coordinator: c1
    coordinators:
        c1: [localhost, 4701]
    active_deployment: cluster
    deployments:
        cluster:
            read_repair: yes
            background_healing: yes
            background_healing_interval: '1d 0:00:00'
            hash:
                buckets_per_key: 2
            buckets:
                b1:
                    n1: [localhost, 5711, 4711]
                    n2: [localhost, 5712, 4712]
                b2:
                    n3: [localhost, 5713, 4713]
                    n4: [localhost, 5714, 4714]
                b3:
                    n5: [localhost, 5715, 4715]
                    n6: [localhost, 5716, 4716]
                b4:
                    n7: [localhost, 5717, 4717]
                    n8: [localhost, 5718, 4718]
                b5:
                    n9: [localhost, 5719, 4719]
                    n10: [localhost, 5720, 4720]
