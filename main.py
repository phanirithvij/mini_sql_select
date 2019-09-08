import sys

import sqlparse

from algos import (aggregate, avg, filter_columns, format_data, join,
                   multiplecols, project)
from parse_q import (full_cols, get_sch_cols, index_by_col, parse_query,
                     read_data, read_metadata)


class SQLEngine():
    def __init__(self, file_name="files/metadata.txt"):
        self.schema = read_metadata(file_name)
        self.tables = list(self.schema.keys())
        self.curschema = {}
        self.distinct = False
        self.relation = None
        self.aggregator = None
        self.aggregator_name = None
        self.aggrecol = None
        self.constraints = None
        self.pairs = []
        self.tableschema = None
        self.query = ""

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
                    tokensa[0].value, self.curtables, self.curschema)
                pair.append(ide1)

                ide2 = index_by_col(
                    tokensa[-1].value, self.curtables, self.curschema)
                pair.append(ide2)

                oper = tokensa[1].value
                pair.append(oper)

                # must lie in the if condition
                self.pairs.append(pair)

    def parse(self, query):
        """
        Parses the query and populates the memory with contents
        Use `engine.run()` to run the last parsed query
        """
        self.query = query
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

        if not self.wild:
            # if wild max len will be 2
            # make it same length in both * and non *
            self.cols = tokens.pop(0)
            if type(self.cols) == sqlparse.sql.Function:
                # aggregate
                self.aggreschema = [self.cols]
                self.aggregator = self.cols.tokens[0].value
                self.aggregator_name = self.aggregator
                if self.aggregator_name not in ['sum', 'avg', 'min', 'max', 'len']:
                    raise Exception(
                        f"'{self.aggregator_name}' no such aggregator")
                exec(f'agg = {self.aggregator}', locals(), globals())
                self.aggregator = agg
                # remove paranthesis
                self.aggrecol = self.cols.tokens[1].tokens[1].value
            elif type(self.cols) == sqlparse.sql.Identifier:
                # one value case and not aggregate
                if len(self.cols.tokens) == 3:
                    # table.A
                    single_value = self.cols.tokens[-1].value
                else:
                    single_value = self.cols.tokens[0].value
                del self.cols
                self.cols = ['']
                self.cols[0] = single_value
                # print(self.cols, "*"*67)
                self.cols = list([x.split('.')[-1] for x in self.cols])
                # print(self.cols, "*"*67)
            else:
                # multi column projection
                self.cols = list(
                    filter(
                        lambda x: type(x) != sqlparse.sql.Token, self.cols))
                self.cols = list([x.value for x in self.cols])
                self.cols = list([x.split('.')[-1] for x in self.cols])

        # print(tokens[-1], type(tokens[-1]))
        if len(tokens) < 1 or len(tokens) > 3:
            raise Exception("Invalid sql syntax", (self.query))

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

        self.tableschema = get_sch_cols(self.curschema)

        if self.wild:
            self.cols = self.curschema.values()
            d = []
            for i in self.cols:
                d += i
            self.cols = d

        # check of all attributes exist in the schema
        if not self.aggrecol:
            colsd = [x.split('.')[-1] for x in self.tableschema]
            # print(colsd)
            # print(self.cols)
            for col in self.cols:
                if col not in colsd:
                    raise Exception(f"'{col}' attribute doesn't exist")

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
                    val = list(filter(lambda x: x.endswith(col), new_cols))
                    if len(val) == 1:
                        val = val[0]
                        if val != f'{tabe}.{col}':
                            new_cols[new_cols.index(val)] = f'{tabe}.{col}'
                    # else:
                    #     print('Another one', val)

            del self.cols
            self.cols = new_cols

        if type(tokens[-1]) == sqlparse.sql.Where:
            self.constraints = tokens[-1]
        elif type(tokens[-1]) == sqlparse.sql.Comparison:
            # spelling mistake in where
            raise Exception("'where' is spelled wrongly in the Query")
        self.parse_constraints()

        self.raw = [read_data(f'files/{x}.csv') for x in self.curtables]

        if self.cols == []:
            print([])
            exit(0)

        # print('distinct', self.distinct)
        # print('aggregate', self.aggregator, self.aggrecol)
        # print('cols', self.cols)
        # print('tables', self.curtables)
        # print('relation', self.relation)
        # print('pairs', self.pairs)

    def run(self):

        if len(self.curtables) >= 2:
            joined = join(*self.raw)
        else:
            joined = self.raw[0]
        # size is (0, num_cols1, num_cols1+num_cols2, num_cols1+num_cols2+num_cols3 ...)
        # size = [0, len(table1[0]), len(table1[0]) + len(table2[0])]
        size = [0]
        for table in self.raw:
            lates = size[-1]
            size.append(len(table[0]) + lates)

        data = filter_columns(
            joined,
            pairs=self.pairs,
            size=size,
            relation=self.relation
        )

        if self.aggrecol:
            orig = self.aggrecol
            self.aggrecol = get_sch_cols(self.curschema, *self.curtables)
            val = list(filter(lambda x: x.endswith(orig), self.aggrecol))
            if len(val) == 1:
                self.aggrecol = val[0]
            else:
                raise Exception(f"Aggregation not possible with {orig}")
            col = index_by_col(self.aggrecol, self.curtables, self.curschema)
            data = aggregate(data, self.aggregator, col, size=size)
            data = [data]
            schema = [self.aggregator_name +
                      str(self.aggreschema[0].tokens[-1])]
        else:
            cols = []
            for col in self.cols:
                colid = index_by_col(col, self.curtables, self.curschema)
                cols.append(colid)
            multi = multiplecols(data, cols, size, self.tableschema)

            schema = multi[0]
            data = multi[1:]

        if self.distinct:
            data = dict.fromkeys(map(lambda x: tuple(x), data))
            data = list(data)

        project(data, schema)


if __name__ == '__main__':
    engine = SQLEngine()
    querylist = sys.argv[1:]
    for query in querylist:
        # print(query)
        engine.parse(query)
        engine.run()
