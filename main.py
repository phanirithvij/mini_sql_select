from parse_q import read_metadata
from parse_q import parse_query
import sys


class SQLEngine():
    def __init__(self, file_name):
        self.schema = read_metadata(file_name)


if __name__ == '__main__':
    engine = SQLEngine("files/metadata.txt")
    print(engine.schema)
    querylist = sys.argv[1:]
    for tok in parse_query(';'.join(querylist)):
        print(tok.tokens)
