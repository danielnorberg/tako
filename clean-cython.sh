#!/bin/sh
# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

cd $(dirname $0)
find . -type f -name "*.c" -exec rm {} \;
find . -type f -name "*.so" -exec rm {} \;
find . -type f -name "*.pyc" -exec rm {} \;
rm -rf build
