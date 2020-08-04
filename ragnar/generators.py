import csv


def gen_strip(cad):
    return cad.strip()


def read_file(filename):
    with open(filename) as f:
        for line in f:
            yield line


def read_csv(filename):
    with open(filename, "r") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            yield row
