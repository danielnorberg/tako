# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

cdef class Store(object):
    cdef unsigned long operation_counter
    cdef str filepath
    cdef object db
    cdef object flusher

    cpdef open(self)
    cpdef close(self)
    cpdef _flush(self)
    cpdef set(self, object key, object value, timestamp=*)
    cpdef set_timestamped(self, key, timestamped_value)
    cpdef unpack_timestamped_data(self, data)
    cpdef pack_timestamped_data(self, data, timestamp)
    cpdef read_timestamp(self, data)
    cpdef get_timestamped(self, key)
    cpdef get(self, key)
    cpdef _jump(self, cur, start)
    # cpdef _range(self, cur, start, end)
    # cpdef get_key_value_range(self, start_key, end_key)
    # cpdef get_key_range(self, start_key, end_key)
    cpdef begin(self)
    cpdef commit(self)
