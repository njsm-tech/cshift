def enum_str2pb(s: str, pb_enum):
    key = s.upper()
    if not hasattr(pb_enum, key):
        raise ValueError('%s has no attribute %s' % (pb_enum, key))
    return getattr(pb_enum, s.upper())