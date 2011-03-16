import re
from datetime import timedelta

def parse_timedelta(s):
    if s is None:
        return None
    d = re.match(
            r'((?P<days>\d+)\s*(days|day|d)\s*,?)?\s*((?P<hours>\d+)\s*:\s*'
            r'(?P<minutes>\d+)\s*:\s*(?P<seconds>\d+))?\s*',
            str(s)).groupdict(0)
    return timedelta(**dict(( (key, int(value))
                              for key, value in d.items() )))

