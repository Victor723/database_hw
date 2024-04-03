#!/usr/bin/env python

import argparse
from pyspark.sql import SparkSession

from format import vote_fields, vote_path, explain_fields, explain_path,\
    extract_rows


def load_module(module):
    module_path = module
    return __import__(module_path, fromlist=[module])


if __name__ == '__main__':

    ######################################################################
    parser = argparse.ArgumentParser()
    parser.add_argument('datadir', type=str, help='data directory')
    parser.add_argument('part', type=str, choices=['a', 'b', 'c', 'd'],
                        help='part of the problem')
    parser.add_argument('method', type=str, choices=['dataframe', 'mapreduce'])
    args = parser.parse_args()

    ######################################################################
    spark = SparkSession\
        .builder\
        .appName("Analysis of Legistators' Excuses")\
        .getOrCreate()
    sc = spark.sparkContext

    ######################################################################
    m = load_module('{}_{}'.format(args.part, args.method))
    print('====================== Part ({}); Method: {} ========================='
          .format(args.part, args.method))
    if args.method == 'dataframe':
        # setting up DataFrames for each type of data:
        df_votes = sc.wholeTextFiles(args.datadir + '/vote-*.txt')\
                     .flatMap(lambda x: extract_rows(x[1], vote_fields, vote_path))\
                     .toDF(vote_fields)
        df_explain = sc.wholeTextFiles(args.datadir + '/explain-*.txt')\
                       .flatMap(lambda x: extract_rows(x[1], explain_fields, explain_path))\
                       .toDF(explain_fields)
        m.query(df_votes, df_explain).show(20, truncate=False)
    else:
        # setting up one RDD that contains all the input:
        rdd = sc.union([
            sc.wholeTextFiles(args.datadir + '/vote-*.txt')
            .flatMap(lambda x: extract_rows(x[1], vote_fields, vote_path)),
            sc.wholeTextFiles(args.datadir + '/explain-*.txt')
            .flatMap(lambda x: extract_rows(x[1], explain_fields, explain_path))
        ])
        for row in rdd.flatMap(m.map)\
                      .groupByKey()\
                      .flatMap(m.reduce)\
                      .sortBy(m.sort_key)\
                      .take(20):
            print('|'.join(str(field) for field in row))
    print('======================================================================')
