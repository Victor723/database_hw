import json


# relevant fields in a vote object:
vote_fields = ('vote_uri', 'question', 'description', 'date', 'time', 'result')
vote_path = ['results', 'votes']
# relevant fields in a vote object:
explain_fields = ('member_id', 'name', 'state', 'party', 'vote_api_uri',
                  'date', 'text', 'category')
explain_path = ['results']


def extract_rows(json_input, fields, path):
    '''Convert the JSON result from a ProPublica API call into a series of
    Python objects representing individual result records, one at a
    time.  For a successful ProPublica API call, the array of
    individual result records can be retrieved by following the given
    path (a list of string labels).  For each result record, and given
    the list of fields of interest, this method returns a tuple, whose
    components are the values for the specified fields in order.

    '''
    result_obj = json.loads(json_input)
    rows = list()
    for step in path:
        if step not in result_obj:
            return rows
        else:
            result_obj = result_obj[step]
    for record in result_obj:
        rows.append(tuple(record[field] for field in fields))
    return rows
