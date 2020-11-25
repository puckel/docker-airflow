def fast_checksum(s):
    # Fast checksum. We don't need it to be too unique, just unique enough to tell us if it's changed
    checksum = hash(s).to_bytes(8, 'big', signed=True).hex()
    return checksum
