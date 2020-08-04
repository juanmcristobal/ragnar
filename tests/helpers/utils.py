from random import randint

from faker import Faker


def generate_mock_file(filename, lines, func_list):
    fake = Faker()
    data = []
    obj = {"filename": filename, "lines": lines}
    for _ in range(0, lines):
        data.append(
            [
                str(randint(1, 10)),
                fake.name(),
                fake.address().replace("\n", " - "),
                str(fake.latitude()),
                str(fake.longitude()),
            ]
        )

    for key, func in func_list.items():
        obj.setdefault(key, len([i for i in filter(func, data)]))

    with open(filename, "w") as randomfile:
        for line in data:
            randomfile.write("%s\n" % ",".join(line))

    return type("MockFile", (object,), obj)


def generate_mock_list(records, seed, isdict=False):
    fake = Faker()
    Faker.seed(seed)
    header = ["idx", "date1", "date2", "uuid", "file", "user_agent", "credit_card_full"]
    for idx in range(0, records):
        data = [
            str(idx),
            fake.date_between(start_date="-7y", end_date="today").strftime(
                "%b %d %Y %H:%M:%S"
            ),
            fake.date_between(start_date="-1y", end_date="today").strftime(
                "%b %d %Y %H:%M:%S"
            ),
            fake.uuid4(),
            fake.file_path() + fake.file_name(),
            fake.user_agent(),
            fake.credit_card_full(),
        ]
        if isdict:
            yield dict(zip(header, data))
        else:
            yield data
