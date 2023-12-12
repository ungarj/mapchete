def pretty_bytes(count: float, round_value: int = 2) -> str:
    """Return human readable bytes."""
    out = ""

    for measurement in [
        "bytes",
        "KiB",
        "MiB",
        "GiB",
        "TiB",
        "PiB",
        "EiB",
        "ZiB",
        "YiB",
    ]:
        out = f"{round(count, round_value)} {measurement}"
        if count < 1024.0:
            break
        count /= 1024.0

    return out


def pretty_seconds(elapsed_seconds: float, round_value: int = 3):
    """Return human readable seconds."""
    minutes, seconds = divmod(elapsed_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes:
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        return f"{round(seconds, round_value)}s"
