import sys

import sqlparse

from algos import format_data, avg
from parse_q import (full_cols, index_by_col, parse_query, read_data,
                     read_metadata)

class SQLEngine():
    def __init__(self, file_name):
        self.schema = read_metadata(file_name)
        self.tables = list(self.schema.keys())
        self.curschema = {}
        self.distinct = False
        self.relation = None
        self.aggregator = None
        self.aggrecol = None
        self.constraints = None
        self.pairs = []

    def parse_constraints(self):
        if self.constraints:
            self.constraints = filter(lambda x: x.ttype !=
                                      sqlparse.tokens.Whitespace, self.constraints)
            self.constraints = list(self.constraints)
            # remove where
            self.constraints[:] = self.constraints[1:]
        else:
            return

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

    def parse(self, query):
        tokens = parse_query(query)[0].tokens

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
        print(tokens)

        if not self.wild:
            # if wild max len will be 2
            # make it same length in both * and non *
            self.cols = tokens.pop(0)
            if type(self.cols) == sqlparse.sql.Function:
                # aggregate
                self.aggregator = self.cols.tokens[0].value
                exec(f'agg = {self.aggregator}', locals(), globals())
                print(agg)
                # remove paranthesis
                self.aggrecol = self.cols.tokens[1].tokens[1].value
            elif type(self.cols) == sqlparse.sql.Identifier:
                # one value case and not aggregate
                single_value = self.cols.tokens[0].value
                del self.cols
                self.cols = ['']
                self.cols[0] = single_value
            else:
                # multi column projection
                self.cols = list(
                    filter(
                        lambda x: type(x) != sqlparse.sql.Token, self.cols))
                self.cols = list([x.value for x in self.cols])

        if len(tokens) < 1 or len(tokens) > 3:
            raise Exception("Invalid sql syntax", (tokens))

        self.curtables = tokens[0]
        if type(self.curtables) == sqlparse.sql.Identifier:
            single_table = self.curtables.tokens[0].value
            del self.curtables
            self.curtables = ['']
            self.curtables[0] = single_table
        else:
            self.curtables = list(
                filter(
                    lambda x: type(x) != sqlparse.sql.Token, self.curtables))
            self.curtables = list([x.value for x in self.curtables])

        for tab in self.curtables:
            if tab not in self.tables:
                raise Exception(f"'{tab}' table doesn't exist in the DB")

        # setup the current schema
        for k, v in self.schema.items():
            if k in self.curtables:
                self.curschema[k] = v

        if self.wild and not self.aggrecol:
            # duplicate columns still needed if wildcard
            # not aggregating
            self.cols = full_cols(self.curschema)
        elif not self.aggrecol:
            # purify cols after getting the table names
            # A to table.A
            new_cols = []
            enc = []
            for col in self.cols:
                # special case when mixed col names come in
                # eg: A, table2.B, table2.D from table1, table2
                tabe = None
                spl = col.split('.')
                col = spl[-1]
                if len(spl) == 2:
                    tabe = spl[0]
                for table in self.curtables:
                    if col in self.schema[table] and col not in enc:
                        enc.append(col)
                        new_cols.append(f'{table}.{col}')
                if tabe:
                    val = list(filter(lambda x: x.endswith(col), new_cols))[0]
                    if val != f'{tabe}.{col}':
                        new_cols[new_cols.index(val)] = f'{tabe}.{col}'

            del self.cols
            self.cols = new_cols

        if type(tokens[-1]) == sqlparse.sql.Where:
            self.constraints = tokens[-1]

        self.parse_constraints()

        self.raw = [read_data(f'files/{x}.csv') for x in self.curtables]
        self.data = format_data(self.curschema, self.raw)

        if self.cols == []:
            raise Exception("Sql query invalid")

        print('distinct', self.distinct)
        print('cols', self.cols)
        print('tables', self.curtables)
        print('relation', self.relation)
        print('pairs', self.pairs)

    def run(self):
        pass


if __name__ == '__main__':
    engine = SQLEngine("files/metadata.txt")
    # print(engine.schema)
    querylist = sys.argv[1:]
    for query in querylist:
        engine.parse(query)
