"""Histogram of daily returns. Presentation only: every metric comes from the marts."""
from __future__ import annotations

import math
from datetime import date

from api.schemas import DayReturn, ReturnBin, ReturnDistribution

BIN_WIDTH = 0.005  # half a percentage point per bar


def return_distribution(days: list[tuple[date, float]], bin_width: float = BIN_WIDTH) -> ReturnDistribution:
    """Bins of `bin_width` aligned on zero, so a bar never straddles gains and losses."""
    returns = [r for _, r in days]
    if not returns:
        return ReturnDistribution(
            bin_width=bin_width, bins=[], sessions=0, mean=None, std=None,
            share_negative=None, worst_day=None, best_day=None,
        )

    first = math.floor(min(returns) / bin_width)
    last = math.floor(max(returns) / bin_width)
    counts = [0] * (last - first + 1)
    for r in returns:
        counts[math.floor(r / bin_width) - first] += 1

    n = len(returns)
    mean = sum(returns) / n
    std = math.sqrt(sum((r - mean) ** 2 for r in returns) / (n - 1)) if n > 1 else None
    worst = min(days, key=lambda d: d[1])
    best = max(days, key=lambda d: d[1])
    return ReturnDistribution(
        bin_width=bin_width,
        bins=[
            ReturnBin(
                lower=round((first + i) * bin_width, 10),
                upper=round((first + i + 1) * bin_width, 10),
                sessions=count,
            )
            for i, count in enumerate(counts)
        ],
        sessions=n,
        mean=mean,
        std=std,
        share_negative=sum(r < 0 for r in returns) / n,
        worst_day=DayReturn(date=worst[0], daily_return=worst[1]),
        best_day=DayReturn(date=best[0], daily_return=best[1]),
    )
