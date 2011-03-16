# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from setuptools import setup

setup(
    name='tako',
    version='0.0.7',
    packages=['tako', 'tako.utils'],
    scripts=[
        'bin/tako-node',
        'bin/tako-coordinator',
        'bin/tako-proxy'
    ],

    install_requires = [
        'socketless >= 0.3.1',
        'pytc >= 0.8',
        'argparse',
        'pyyaml',
        'simplejson',
        'urllib3'
        ],
    zip_safe=True,

    author = "Daniel Norberg",
    author_email = "daniel.norberg@gmail.com",
    url = "https://github.com/danielnorberg/tako/",
    description = "Tako: A distributed data store.",
        long_description =      """\
        Tako is a distributed key-value data store. It aims to provide high
        scalability and availability through a shared nothing architecture,
        consistent hash based data partitioning, read repair with time
        stamping and live migration. An included coordinator server can be
        used as a single point of configuration to distribute cluster
        configuration to tako nodes in a cluster.

        Tako includes a http proxy server that can be used to
        interface with a tako cluster using normal HTTP GET/POST.

        Within a cluster, tako nodes communicate using a binary
        protocol and the socketless and syncless libraries.

        Tako makes use of libev/libevent/kqueue/kpoll if present.
        Tokyo Cabinet is used for data storage.
        """,
    license="Apache License, Version 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Environment :: No Input/Output (Daemon)",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Unix",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
