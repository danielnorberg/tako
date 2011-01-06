#!/bin/sh
cd $(dirname $0)
python cython-setup.py build_ext --inplace
# if [ "$1" != "--leave-so" ]
#   then
#     find . -type f -name "*.so" -exec rm {} \;
# fi
# rm -rf build

