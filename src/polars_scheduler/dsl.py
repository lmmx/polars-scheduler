periods = "yearly monthly weekly daily".split()
periods_abbrev = "y mo w d h m s".split()


def freq(frequency: str) -> None:
    """Validate frequencies against a simple DSL.

    For example: "daily", "1x daily", "1x /d", "1x /1d", etc.
    """
    prefix, sep, suffix = frequency.partition(" ")
    times = prefix if sep else "1x"
    period = suffix if sep else prefix
    if times.endswith("x"):
        assert times.removesuffix("x").isnumeric(), f"Times {times} is not like '2x'"
    if period not in periods:
        assert period.startswith("/"), f"Period {period} is not like 'daily' or '/1d'"
        period_per = period.removeprefix("/")
        found_abbrev = ""
        for period_abbrev in periods_abbrev:
            if period_per.endswith(period_abbrev):
                found_abbrev = period_abbrev
                break
        assert found_abbrev, f"Period {period} is not recognised"
        if period_per == found_abbrev:
            n_per = 1
        else:
            n_per_prefix = period_per.removesuffix(found_abbrev)
            assert n_per_prefix.isnumeric(), f"{n_per_prefix} should be an int"
            n_per = int(n_per_prefix)
    return frequency
