from format import vote_fields, explain_fields


def map(record):
    # REPLACE THE FOLLOWING WITH YOUR OWN IMPLEMENTATION:
    return []


def reduce(record):
    # REPLACE THE FOLLOWING WITH YOUR OWN IMPLEMENTATION:
    return []


def sort_key(record):
    # given a record, construct and return a sort key object that be
    # used for sorting.  specifically, ordering the constructed sort
    # keys in ascending order should put the corresponding records in
    # the desired order.
    # NOTE: due to data quality issue, some state and party values
    # can be None; in those cases, treat None as '' (empty string)
    # for the purpose of ordering.
    # REPLACE THE FOLLOWING WITH YOUR OWN IMPLEMENTATION:
    return ()
