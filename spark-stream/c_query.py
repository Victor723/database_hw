from pyspark.sql import functions as F

def query(stream_sales, stream_prices):
    # construct the stream query:
    # REPLACE THE FOLLOWING WITH YOUR IMPLEMENTATION:
    q = stream_sales\
        .filter(stream_sales.qty >= 0)
    # one of 'append', 'complete', and 'update':
    # REPLACE THE FOLLOWING WITH YOUR CHOICE:
    output_mode = 'append'
    return q, output_mode
