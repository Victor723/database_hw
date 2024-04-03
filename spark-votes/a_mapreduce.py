from format import vote_fields, explain_fields


def map(record):
    print(record)
    if len(record) == len(explain_fields):  # explain record
        return [(record[explain_fields.index('category')], 1)]
    else:  # vote record
        return []


def reduce(record):
    key, vals = record
    return [(key, len(vals))]


def sort_key(record):
    return (-record[1], record[0])
