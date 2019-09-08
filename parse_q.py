import sqlparse
import csv


def parse_query(query):
    return sqlparse.parse(query)


def read_data(file_name):
    with open(file_name, 'r') as f:
        data = csv.reader(f)
        data = list(data)
        for row in data:
            da = map(lambda x: int(x), row)
            row[:] = list(da)
        return data


def full_cols(schema):
    ret = []
    for k, v in schema.items():
        li = list(map(lambda x: f'{k}.{x}', v))
        ret += li
    return ret


def get_sch_cols(metadata, *tables):
    '''
    table, attribute will be combined
    returns a list of str
    like table1.A, table1.B, table1.C
    '''
    if not tables:
        tables = metadata.keys()
    a = []
    for table in tables:
        schema_x = map(lambda x: f'{table}.{x}', metadata[table])
        a.append(list(schema_x))
    d = []
    for x in a:
        d[:] += x
    return d


def index_by_col(exact, tables, schema):
    try:
        value = int(exact)
        return value
    except ValueError:
        splat = exact.split('.')
        if len(splat) == 2:
            table, colname = splat
            try:
                tidx = tables.index(table)
                colidx = schema[table].index(colname)
            except ValueError:
                raise Exception(
                    f"No such table '{table}' at where clause")
            return (tidx, colidx)
        else:
            tschema = get_sch_cols(schema, *tables)
            val = list(filter(lambda x: x.endswith(splat[0]), tschema))
            # print(tschema, val, splat[0])
            if len(val) == 0:
                raise Exception(
                    f"Invalid table name '{splat[0]}' at where clause")
            return index_by_col(val[0], tables, schema)


def read_metadata(file_name="files/metadata.txt") -> dict:
    """
    Reads the metadata from a file and returns a dictionary
    """
    data = {}
    insert = False
    table = None
    with open(file_name, 'r') as f:
        content = f.readlines()
        for i, line in enumerate(content):
            # strip off the newline
            content[i] = line[:-1]
            line = line[:-1]

            if table is not None:
                if table not in data:
                    data[table] = []

                if insert:
                    data[table].append(line)

            if line == '<begin_table>':
                insert = True
                table = content[i+1][:-1]
            elif line == '<end_table>':
                insert = False
                table = None

        for k, v in data.items():
            del v[0], v[-1]

    return data


if __name__ == "__main__":
    sch = (read_metadata())
    print(full_cols(sch))
