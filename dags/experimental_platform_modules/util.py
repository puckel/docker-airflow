import hashlib


def fast_checksum(s):
    # Fast checksum. We don't need it to be too unique, just unique enough to tell us if it's changed
    checksum = hashlib.md5(s.encode('utf-8')).hexdigest()[:16]
    return checksum
