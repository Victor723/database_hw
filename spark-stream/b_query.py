from pyspark.sql import functions as F

def query(stream_sales, stream_prices):

    salesAlias = stream_sales.alias("sales")
    pricesAlias = stream_prices.alias("prices")

    salesAlias = salesAlias.withWatermark("timestamp", "10 seconds")
    pricesAlias = pricesAlias.withWatermark("timestamp", "5 seconds")

    joinedStream = salesAlias.join(
        pricesAlias,
        (salesAlias["product"] == pricesAlias["product"]) &
        (salesAlias["timestamp"] >= pricesAlias["timestamp"]) &
        (salesAlias["timestamp"] <= F.expr("prices.timestamp + interval 10 seconds")),
        "inner"
    )

    result = joinedStream.select(
        salesAlias["timestamp"],
        salesAlias["product"],
        salesAlias["qty"],
        (salesAlias["qty"] * pricesAlias["price"]).alias("total_price")
    )

    output_mode = "append"
    return result, output_mode

