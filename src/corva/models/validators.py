def from_ms_to_s(timestamp: int) -> int:
    """Casts Unix timestamp from milliseconds to seconds.

    Casts Unix timestamp from millisecond to seconds, if provided timestamp is in
        milliseconds.
    """

    # 1 January 10000 00:00:00 - first date to not fit into the datetime instance
    if timestamp >= 253402300800:
        timestamp //= 1000

    return timestamp
