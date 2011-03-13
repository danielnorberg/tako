# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

cdef class Store(object):
    cdef unsigned long operation_counter
    cdef str filepath
    cdef object db
    cdef object flusher
    cdef object auto_commit_interval
    cdef object pack_timestamp
    cdef object unpack_timestamp

    cpdef begin(self)
    cpdef commit(self)

    cpdef open(self)
    cpdef close(self)

    cpdef set(self, object key, long timestamp, object value)
    cpdef get(self, key)


    # cpdef _jump(self, cur, start)
    # cpdef _range(self, cur, start, end)
    # cpdef get_key_value_range(self, start_key, end_key)
    # cpdef get_key_range(self, start_key, end_key)
