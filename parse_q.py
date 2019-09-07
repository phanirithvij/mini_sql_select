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


def index_by_col(exact, tables, schema):
    try:
        value = int(exact)
        return value
    except ValueError:
        table, colname = exact.split('.')
        tidx = tables.index(table)
        colidx = schema[table].index(colname)
        return (tidx, colidx)


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
    print(read_metadata())
