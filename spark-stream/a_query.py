from pyspark.sql import functions as F

def query(stream_sales, stream_prices):

    # construct the stream query:
    # REPLACE THE FOLLOWING WITH YOUR IMPLEMENTATION:
    q = stream_sales \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            F.window("timestamp", "10 seconds", "5 seconds"),
            "product") \
        .agg(F.sum("qty").alias("total_qty"))

    # one of 'append', 'complete', and 'update':
    # REPLACE THE FOLLOWING WITH YOUR CHOICE:
    output_mode = 'update'
    # output_mode = 'complete'
    return q, output_mode
