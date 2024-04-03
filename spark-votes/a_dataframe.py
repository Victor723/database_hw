from pyspark.sql import functions as F
from format import vote_fields, explain_fields


def query(df_votes, df_explain):
    # DataFrame[member_id: string, name: string, state: string, party: string, vote_api_uri: string, date: string, text: string, category: string]
    print(df_explain)
    return df_explain.groupBy('category')\
                     .agg(F.count('name'))\
                     .withColumnRenamed('count(name)', 'count')\
                     .sort(['count', 'category'], ascending=[0, 1])
