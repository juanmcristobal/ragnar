
def read_file(filename):
    with open(filename) as f:
        for line in f:
            yield line
