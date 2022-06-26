from datetime import datetime
from random import randrange

from ragnar.objstream import ObjStream
from tests.helpers.utils import generate_mock_list

RECORDS = 100
HEADERS = ["idx", "date1", "date2", "uuid", "file", "user_agent", "credit_card_full"]


def date_func(x):
    return datetime.strptime(x, "%b %d %Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")


def test_applyto_single_column():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed), columns=HEADERS)
    obj.applyto(
        "date1",
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed):
        row[HEADERS.index("date1")] = date_func(row[HEADERS.index("date1")])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_many_applyto_single_column():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed), columns=HEADERS)

    obj.applyto(
        "date1",
        date_func,
    )
    obj.applyto(
        "date2",
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed):
        row[HEADERS.index("date1")] = date_func(row[HEADERS.index("date1")])
        row[HEADERS.index("date2")] = date_func(row[HEADERS.index("date2")])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_applyto_multiple_column():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed), columns=HEADERS)

    obj.applyto(
        ["date1", "date2"],
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed):
        row[HEADERS.index("date1")] = date_func(row[HEADERS.index("date1")])
        row[HEADERS.index("date2")] = date_func(row[HEADERS.index("date2")])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_applyto_indict_single_column():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed, isdict=True))

    obj.applyto(
        "date1",
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed, isdict=True):
        row["date1"] = date_func(row["date1"])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_man_applyto_indict_single_column():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed, isdict=True))

    obj.applyto(
        "date1",
        date_func,
    )
    obj.applyto(
        "date2",
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed, isdict=True):
        row["date1"] = date_func(row["date1"])
        row["date2"] = date_func(row["date2"])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_applyto_indict_multiple_column():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed, isdict=True))

    obj.applyto(
        ["date1", "date2"],
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed, isdict=True):
        row["date1"] = date_func(row["date1"])
        row["date2"] = date_func(row["date2"])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_skipfirst():
    seed = randrange(1000)
    obj = ObjStream(
        generate_mock_list(RECORDS, seed=seed), columns=HEADERS, skip_first=True
    )
    obj.applyto(
        "date1",
        date_func,
    )

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed):
        row[HEADERS.index("date1")] = date_func(row[HEADERS.index("date1")])
        manual_etl.append(row)

    assert manual_etl[1:] == [row for row in obj]


def test_entire_object_list():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed), columns=HEADERS)

    def entire_func(row):
        return date_func(row["date1"])

    obj.applyto("date1", entire_func, entire_object=True)

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed):
        row[HEADERS.index("date1")] = date_func(row[HEADERS.index("date1")])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]


def test_entire_object_dict():
    seed = randrange(1000)
    obj = ObjStream(generate_mock_list(RECORDS, seed=seed, isdict=True))

    def entire_func(row):
        return date_func(row["date1"])

    obj.applyto("date1", entire_func, entire_object=True)

    manual_etl = []
    for row in generate_mock_list(RECORDS, seed=seed, isdict=True):
        row["date1"] = date_func(row["date1"])
        manual_etl.append(row)

    assert manual_etl == [row for row in obj]
