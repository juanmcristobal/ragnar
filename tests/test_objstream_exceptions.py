from datetime import datetime
from random import randrange

import pytest

from ragnar.objstream import ObjStream
from tests.helpers.utils import generate_mock_list

RECORDS = 1000
HEADERS = ["idx", "date1", "date2", "uuid", "file", "user_agent", "credit_card_full"]

date_func = lambda x: datetime.strptime(x, "%b %d %Y %H:%M:%S").strftime(
    "%Y-%m-%d %H:%M:%S"
)


def test_column_exception_in_dict_stream():
    seed = randrange(1000)
    obj = ObjStream(
        generate_mock_list(RECORDS, seed=seed, isdict=True), columns=HEADERS
    )

    obj.applyto(
        "date1", date_func,
    )
    with pytest.raises(Exception):
        for _ in obj:
            pass
