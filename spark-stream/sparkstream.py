import argparse
import multiprocessing
import sys
from datetime import datetime
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def load_module(module):
    module_path = module
    return __import__(module_path, fromlist=[module])

if __name__ == '__main__':

    ######################################################################
    parser = argparse.ArgumentParser()
    parser.add_argument('part', type=str, choices=['a', 'b', 'c'],
                        help='part of the problem')
    parser.add_argument('--duration', type=int, default=120,
                        help='duration in seconds; streams will stop 20 seconds earlier')
    parser.add_argument('--num_products', type=int, default=2,
                        help='number of products')
    parser.add_argument('--delay_prob', type=float, default=0.5,
                        help='delay probability')
    args = parser.parse_args()
    if args.duration < 40:
        print('error: duration should be at least 40 seconds')
        sys.exit(1)
    if args.num_products > 30:
        print('error: number of products is capped at 30')
        sys.exit(1)

    ######################################################################
    spark = SparkSession\
        .builder\
        .appName("Streaming Sales")\
        .getOrCreate()
    sc = spark.sparkContext

    ######################################################################
    dir_in_sales = 'stream-sales'
    dir_in_prices = 'stream-prices'

    gen = load_module('gen_streams')
    gen_process = multiprocessing.Process(
        target=gen.gen_streams,
        args = (
            # starting timestamp, step size (seconds), # steps, # products:
            datetime.fromisoformat('2000-01-01T00:00:00'), 2, (args.duration-20)//2, args.num_products,
            # directory, frequency (one batch every # of steps), generation function,
            # delay probability, max delay (seconds):
            [(dir_in_sales, 2, gen.gen_sales_batch, args.delay_prob, 10),
             (dir_in_prices, 5, gen.gen_prices_batch, args.delay_prob, 5)]))
    gen_process.start()
    sleep(1)

    stream_prices = spark\
        .readStream\
        .format('csv')\
        .schema(T.StructType()\
            .add('timestamp', 'timestamp')\
            .add('product', 'string')\
            .add('price', 'float'))\
        .load(dir_in_prices)

    stream_sales = spark\
        .readStream\
        .format('csv')\
        .schema(T.StructType()\
            .add('timestamp', 'timestamp')\
            .add('product', 'string')\
            .add('qty', 'integer'))\
        .load(dir_in_sales)
    
    ######################################################################
    query = load_module(f'{args.part}_query')
    q, output_mode = query.query(stream_sales, stream_prices)
    standing_query = q\
        .writeStream\
        .format('console')\
        .option('truncate', False)\
        .outputMode(output_mode)\
        .start()
    standing_query.awaitTermination(args.duration)
    standing_query.stop()

    gen_process.join()
