# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

def try_float(s):
    try:
        return float(s)
    except ValueError:
        return None
    except TypeError:
        return None

def try_int(s):
    try:
        return int(s)
    except ValueError:
        return None
    except TypeError:
        return None
