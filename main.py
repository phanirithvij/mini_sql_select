import sys

import sqlparse

from algos import format_data
from parse_q import index_by_col, parse_query, read_data, read_metadata


class SQLEngine():
    def __init__(self, file_name):
        self.schema = read_metadata(file_name)
        self.tables = list(self.schema.keys())
        self.raw = [read_data(f'files/{x}.csv') for x in self.tables]
        self.data = format_data(self.schema, self.raw)
        self.distinct = False
        self.aggregator = None

    def parse_constraints(self):
        if self.constraints:
            self.constraints = filter(lambda x: x.ttype !=
                                      sqlparse.tokens.Whitespace, self.constraints)
            self.constraints = list(self.constraints)
            # remove where
            self.constraints[:] = self.constraints[1:]

        self.pairs = []
        for const in self.constraints:
            if const.is_keyword:
                self.relation = const.value

            if hasattr(const, 'get_real_name'):
                tokensa = list(filter(lambda r: r.ttype !=
                                      sqlparse.tokens.Whitespace, const))
                pair = []
                ide1 = index_by_col(
                    tokensa[0].value, self.tables, self.schema)
                pair.append(ide1)

                ide2 = index_by_col(
                    tokensa[-1].value, self.tables, self.schema)
                pair.append(ide2)

                oper = tokensa[1].value
                pair.append(oper)

                # must lie in the if condition
                self.pairs.append(pair)

    def parse(self, tokens):
        self.wild = False
        for t in tokens:
            if t.value == 'distinct':
                self.distinct = True
                break
        for t in tokens:
            if t.value == '*':
                self.wild = True
                break

        tokens = list(filter(lambda x: type(x) != sqlparse.sql.Token, tokens))

        if self.wild:
            self.cols = '*'
        else:
            self.cols = tokens.pop(0)
        print(self.cols)
        for k in tokens:
            print(k, type(k))
        if len(tokens) <= 1 or len(tokens) > 3:
            raise Exception("Invalid sql syntax")

        self.curtables = tokens[0]
        self.constraints = None

        if type(tokens[-1]) == sqlparse.sql.Where:
            self.constraints = tokens[-1]

        self.relation = None
        self.parse_constraints()

        print(self.distinct)
        print(self.cols)
        print(self.curtables)
        print(self.relation)
        print(self.pairs)

    def run(self):
        pass

if __name__ == '__main__':
    engine = SQLEngine("files/metadata.txt")
    print(engine.schema)
    querylist = sys.argv[1:]
    for query in querylist:
        # x = sqlparse.format(query, reindent=True)
        # print(x.splitlines())
        tok = parse_query(query)[0]
        engine.parse(tok.tokens)
