Tako
====
Tako is a distributed key-value data store. It aims to provide high scalability and availability through a shared nothing architecture, data partitioning using consistent hashing, read repair with time stamping, automatic background healing and live migration. An included coordinator server can be used as a single point of configuration to distribute cluster configuration to Tako nodes in a cluster.

Tako includes a http proxy server that can be used to interface with a Tako cluster using normal HTTP GET/POST.

Within a cluster, Tako nodes communicate using a binary protocol and the socketless and syncless libraries.

Tako makes use of libev/libevent/kqueue/kpoll if present.
Tokyo Cabinet is used for data storage.

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

Next, the virtual environment that will host the Tako installation.
(Note: A default setup of Tako will need write permissions to this directory.)

::

    $ virtualenv tako

Lastly, install ``tako`` and its dependencies.

::

    $ cd tako
    $ bin/pip install tako

This concludes the installation. You can now run a local test cluster using ``tako-cluster`` or use ``tako-node``, ``tako-coordinator`` and ``tako-proxy`` to run a customized setup. For a a test run walkthrough, continue with *Test Run* below.


Test Run
========

There's lots of data sets out there that can be used to experiment with a Tako cluster, but I like music so I'm going to use the million song dataset subset. If you have a lot of time and diskspace and/or machines, try out the full data set and let me know about it =)

First, download the million song subset. The infochimps mirror might be faster.

    http://labrosa.ee.columbia.edu/millionsong/

    http://www.infochimps.com/datasets/the-million-song-dataset-10k-songs-subset

Using tako-cluster we can quickly get a Tako cluster up and running on a single machine. I'll use the local_cluster.yaml with a proxy on port 8080.

Note: running a sizable tako cluster locally uses a lot of disk space. Tako does not currently take well to running out of disk space. The test run below requires around 20GB free disk space.

First download a configuration file from the Tako github repository and start the local cluster::

    # Download configuration file
    mkdir etc
    wget --no-check-certificate https://github.com/danielnorberg/tako/raw/master/examples/local_cluster.yaml -O etc/local_cluster.yaml

    # Start the local tako cluster
    bin/tako-cluster etc/local_cluster.yaml -p 8080

Now we'll populate the Tako cluster using the dataset and then pull it back out again. If you're running a different Tako cluster setup, simply adjust the proxy address and port below. (Note: If running a local cluster like this, the performance and throughput suffers so you'll probably want to get a cup of coffee while you wait for the import and export operations to complete.)

Do this in a second terminal::

    # Unpack the dataset
    tar xzf millionsongsubset.tar.gz

    # Upload the dataset into the Tako cluster
    for f in `find MillionSongSubset -name '*.h5'`; do wget -nv -O /dev/null --post-file=$f http://localhost:8080/values/$(basename $f); done

    # Download the dataset again...
    mkdir fetched
    for f in `find MillionSongSubset -name '*.h5'`; do wget -P fetched -nv http://localhost:8080/values/$(basename $f); done

    # ...and compare all the files, making sure that they survived the roundtrip intact.
    for f in `find MillionSongSubset -name '*.h5'`; do if cmp $f fetched/$(basename $f); then echo $f: Identical; else echo $f: Differing; fi done

Done! Now you can continue experimenting with other data sets. If you want to start over, simply shut down the cluster and remove the ``tako/var`` directory to go back to a clean install or remove the ``tako`` directory to uninstall Tako.

Thanks for trying out Tako! Let me know if something broke =)

Proxy Data Access
=================

Set/Get
-------
As illustrated by the *Test Run* walkthrough, data in a Tako cluster can be accessed through a proxy server by GET and POST to a URL of the form::

    http://tako-proxy-server.domain:port/values/key

E.g. for the imaginary key ``/users/8ea83457738064f32db4b1b2bcf3e8b192846d72/playlists/17``:

    http://tako-proxy-server.domain:port/values/users/8ea83457738064f32db4b1b2bcf3e8b192846d72/playlists/17

Stat
----
Statting, or just getting the timestamp of a value in the cluster can be done by GET request to this url:

    http://proxy-server.domain:port/stat/key

E.g. for the imaginary key ``/users/8ea83457738064f32db4b1b2bcf3e8b192846d72/playlists/17``:

    http://tako-proxy-server.domain:port/stat/users/8ea83457738064f32db4b1b2bcf3e8b192846d72/playlists/17


Key Concepts
============

Key/Value with Timestamps
-------------------------
Tako stores key/value pairs with timestamps and provides two operations: get and set (GET and POST).

Nodes, Proxy, Coordinator
---------------------------
Machines in a Tako cluster are organized into nodes, proxies and coordinator(s).

Nodes store all the data in the cluster. They form the bulk of a Tako cluster and function autonomously, needing only a cluster configuration file to operate fully. Nodes also include an http server enabling each node to double as a data store interface to the entire cluster.

Proxies are used to HTTP POST and GET key/values into and out of the Tako cluster. They act as clients on behalf of external systems, using the internal binary protocol to communicate directly with the actual nodes within the cluster. A typical Tako setup will utilize standard HTTP server load-balancing devices to distribute requests among the set of proxy servers.

Coordinators simply distribute the configuration file to the nodes and proxies, acting as a convenient single point of configuration. Both nodes and proxies cache and persist the cluster configuration locally and are as such not dependent on the coordinator(s) being online. Coordinators are normally only needed during initial setup of a cluster and during subsequent reconfiguration.

Consistent Hashing
------------------
The data in a Tako cluster is partitioned using consistent hashing. This provides a number of beneficial features. Firstly, just by knowing the configuration of the cluster anyone can find out where the data for a particular key is stored without asking a central server. The coordinator server simply distributes the configuration data and all nodes can continue functioning even if the coordinator is down. Secondly, adding or removing nodes doesn't entail spending a lot of time repartitioning the data, thus enabling live migration.

Tako nodes in a cluster are organized into buckets and key/value data is then hashed into these buckets. The nodes in a bucket are mirrors. A only needs to communicate with its mirror nodes and nodes in its neighbor buckets. The number of neighbor buckets has an upper limit of couple of hundreds (depending on the hash configuration parameters) regardless of the size of the cluster, which  ensures that even for massive clusters of thousands or tens of thousands of machines, a node can keep persistent connections to its peers.

Read Repair & Background Healing
--------------------------------
When receiving a request for a value, a node will query its peers for timestamps for that key. If any of its peers has data with a newer timestamp, it will fetch the most recent value from that peer, store it, distribute it to any peers that had older timestamps and return it. The background healing mechanism takes this a step further by simply providing a task that runs on every node and periodically iterating through all key/value pairs of node and applying the above read repair operation. This eliminates the need to use separate logs to keep track of data to distribute and is very robust when compared to other replication mechanisms such as master/slave replication. As part of the background healing, key/values are also garbage collected.


Operation
=========

This describes Tako cluster operation at a conceptual level.

Maintenance
-----------

Tako is designed to not need maintenance downtime. However, Tako does not configure itself. Reconfiguring a cluster by e.g. adding nodes to handle more traffic/data or replace broken machines entails modifying the configuration file and either using the coordinator server to distribute the new configuration to all nodes or distributing it manually through other means.

The background healing mechanism cleans out garbage from nodes and distributes data within the cluster. This process, if enabled, is entirely automatic and one only need to take care to let at least one healing cycle run its course between cluster reconfigurations where nodes are removed the ensure that all inserted key/values are preserved. Adding nodes to a cluster can be done at any time without waiting for the healing mechanism to complete.

Migration
---------

Migration is performed in two steps.

First one includes an extra deployment in the cluster configuration file, giving one *active deployment* and one *target deployment*. The *active deployment* describes the current cluster configuration that one wants to migrate *from* and the *target deployment* describes the new cluster configuration that one wants to migrate *to*. Essentially, this causes two consistent hashes to be used for purposes of data partitioning and routing, request distribution, read repair and background healing. I.e., when looking up the sets of buckets and nodes for a specific key, the union of the lookup results in both consistent hashes is used.

The second step is to let the background healing mechanism run at least one cycle and then promote the *target deployment* to *active deployment*. The previous *active deployment* can be removed from the configuration file.

Usage Reference
===============

A complete reference manual needs to be written. For now the best approach to understand tako is to simply go through the *Test Run* and then experiment freely.

Developing
==========

Start out by looking at ``tako/nodeserver.py``, it is the heart of Tako and implements most of the interesting parts of the system.

Stability
=========

Tako is of pre-alpha quality, built using a lot of unstable components and should not be used in a live system. Tako will locate and delete your most sensitive and critical data as well as cause computers it is installed on to spontaneously combust. Tako installed on servers in data centers is a sign of the coming apocalypse. You have been warned.

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
