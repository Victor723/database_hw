from pyspark.sql import functions as F
from format import vote_fields, explain_fields


def query(df_votes, df_explain):
    return df_votes.join(df_explain.select('vote_api_uri', 'name'),
                         df_votes.vote_uri == df_explain.vote_api_uri, 'left_outer')\
                   .groupBy('vote_uri', 'date', 'time', 'question', 'description', 'result')\
                   .agg(F.count('name'), F.collect_list('name'))\
                   .withColumnRenamed('count(name)', 'count')\
                   .withColumnRenamed('collect_list(name)', 'names')\
                   .sort(['count', 'date', 'time'], ascending=[0, 0, 0])\
                   .select('vote_uri', 'date', 'time', 'question', 'description', 'result',
                           'count', 'names')
