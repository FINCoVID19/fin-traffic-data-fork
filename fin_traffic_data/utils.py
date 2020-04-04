import datetime
from typing import Iterator

def daterange(start_date: datetime.date, end_date: datetime.date) -> Iterator[datetime.date]:
    dt = datetime.timedelta(days=1)
    for n in range((end_date-start_date).days):
        yield start_date + n*dt
