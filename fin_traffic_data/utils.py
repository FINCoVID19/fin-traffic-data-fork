import datetime
from typing import Iterator, Tuple


def daterange(start_date: datetime.date,
              end_date: datetime.date) -> Iterator[datetime.date]:
    dt = datetime.timedelta(days=1)
    for n in range((end_date - start_date).days):
        yield start_date + n * dt


def datetimerange(start_datetime: datetime.datetime,
                  end_datetime: datetime.datetime, delta_t: datetime.timedelta
                  ) -> Iterator[Tuple[datetime.datetime, datetime.datetime]]:
    tb = start_datetime
    te = start_datetime + delta_t

    while te <= end_datetime:
        yield (tb, te)
        tb = te
        te += delta_t
    if tb != end_datetime:
        yield (tb, end_datetime)


def compute_daterange_overlap(dr1_begin, dr1_end, dr2_begin, dr2_end):
    rng1 = set(daterange(dr1_begin, dr1_end))
    rng2 = set(daterange(dr2_begin, dr2_end))
    return list(rng1.intersection(rng2))
