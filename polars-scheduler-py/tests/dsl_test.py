from polars_scheduler.dsl import freq


def test_frequency():
    freq("daily")
    freq("1x daily")
    freq("1x /d")
    freq("1x /1d")

    freq("2x daily")
    freq("10x daily")

    freq("weekly")
    freq("1x /w")
    freq("10x /1w")

    freq("/1d")
    freq("/mo")
    freq("/y")
