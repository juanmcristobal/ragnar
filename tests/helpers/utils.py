from random import randint

from faker import Faker


def generate_mock_file(filename, lines, func_list):
    fake = Faker()
    data = []
    obj = {'filename': filename, 'lines': lines}
    for _ in range(0, lines):
        data.append([str(randint(1, 10)),
                     fake.name(),
                     fake.address().replace("\n", " - "),
                     str(fake.latitude()),
                     str(fake.longitude())])

    for key, func in func_list.items():
        obj.setdefault(key, len([i for i in filter(func, data)]))

    with open(filename, 'w') as randomfile:
        for line in data:
            randomfile.write('%s\n' % ','.join(line))

    return type('MockFile', (object,), obj)
