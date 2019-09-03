import sqlparse


def read_metadata(filename="metadata.txt"):
    with open(filename, 'r') as f:
        print(f.read())
