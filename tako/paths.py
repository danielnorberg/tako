# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import os, sys

def parent(path):
    assert path
    return os.path.dirname(path)

def find_root(root_name):
    root = parent(os.path.abspath(__file__))
    while os.path.basename(root) != root_name:
        if root == '/':
            raise Exception('Project root not found!')
        root = parent(root)
    return root

def path(_path):
    """docstring for path"""
    return os.path.join(home, _path)

home = parent(find_root('tako'))
data = os.path.join(home, 'var', 'data')
log = os.path.join(home, 'var', 'log')

def setup():
    sys.path.append(home)
