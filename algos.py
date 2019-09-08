from parse_q import read_data, read_metadata


class OperationError(Exception):
    pass


def format_data(raw, schema):
    a = []
    for r, s in zip(schema, raw):
        a.append([r, s])
    return a


def project(table, schema):
    schema_x = schema
    print(','.join(schema))
    for t in table:
        t = map(lambda x: str(x), t)
        t = list(t)
        print(','.join(t))


def aggregate(final, aggregator, column, size):
    if aggregator is None:
        return final
    res = []
    for k in final:
        r = k[size[column[0]] + column[1]]
        res.append(r)
    return [aggregator(res)]


def multiplecols(final, cols, size, schema):
    res = []
    for k in [schema] + final:
        # print(k)
        r = []
        for row in cols:
            idx = size[row[0]]+row[1]
            # print(row, size, k)
            r.append(k[idx])
        res.append(r)
    return res


def avg(l):
    if len(l) == 0:
        raise OperationError("avg is not defined for an empty list")
    return sum(l) / len(l)


def join(*tables):
    # working
    if len(tables) == 1:
        return tables[0]
    if len(tables) == 2:
        res = []
        for x in tables[0]:
            for y in tables[1]:
                temp = []
                temp[:] = x
                temp.extend(y)
                res.append(temp)
        return res
    x = join(tables[0], tables[1])
    return join(x, *tables[2:])


# working
def filter_columns(tables, **kwargs):
    relation = kwargs.get('relation')  # and | or | nothing
    pairs = kwargs.get('pairs')
    size = kwargs.get('size')
    if pairs == [] or pairs is None:
        return tables
    if pairs is not None:
        if len(pairs) == 1 and relation is not None:
            raise OperationError(
                f'Requested "{relation}" with just one table')
    if relation == 'and':
        filtered = filter_columns(tables, pairs=[pairs[0]], size=size)
        return filter_columns(filtered, pairs=[pairs[1]], size=size)
    elif relation == 'or':
        filtered = filter_columns(tables, pairs=[pairs[0]], size=size)
        filtered_two = filter_columns(tables, pairs=[pairs[1]], size=size)
        # union filter
        data = dict.fromkeys(map(lambda x: tuple(x), filtered + filtered_two))
        data = list(data)

        # print(filtered, filtered_two)
        return data
    else:
        # relation is None so a single table
        pair = pairs[0]
        op = pair[-1]
        if op == '=':
            op = '=='
        ans = []
        for k in tables:
            inde1 = pair[0][0]
            atrr1 = pair[0][1]
            operand1 = k[size[inde1] + atrr1]
            if type(pair[1]) == tuple:
                # two tables
                inde2 = pair[1][0]
                atrr2 = pair[1][1]
                operand2 = k[size[inde2] + atrr2]
            else:
                # one table with constraints
                operand2 = pair[1]
            # print(f'valid = ({operand1} {op} {operand2})')
            exec(f'valid = ({operand1} {op} {operand2})', locals(), globals())
            if valid:
                ans.append(k)
        return ans



if __name__ == '__main__':
    schema = read_metadata()
    schema = get_sch_cols(schema)
    table1 = read_data('files/table1.csv')
    table2 = read_data('files/table2.csv')
    joined = join(table1, table2)
    size = [0, len(table1[0]), len(table2[0])]
    # [[1, 2], [1, 29], [1, 22]] ,
    # [
    #     [2, 3, 4],
    #     [3, 4, 5],
    #     [5, 6, 7]
    # ]
    # size is (0, num_cols1, num_cols1+num_cols2, num_cols1+num_cols2+num_cols3 ...)
    data = filter_columns(
        joined,
        pairs=[
            [(0, 0), 322, '>='],
            [(0, 1), 811, '<=']
        ],
        size=size,
        relation='or'
    )

    # if
    aggre = aggregate(data, max, (0, 1), size=size)
    aggre = [aggre]
    # else
    # multi = multiplecols(aggre, [(0, 1), (1, 0)], size, schema)

    # schema = multi[0]
    # multidata = multi[1:]

    # # if distinct
    # multidata = set(map(lambda x: tuple(x), multidata))
    # project(multidata, schema)

    # if select *
    project(aggre, schema)
