from SparkBD import SparkBD

spark = SparkBD("airport")

spark.get_rdd("airports.csv")

spark.filter(lambda l: l.split(',')[3] == 'United States')

spark.filter(lambda l: float(l.split(',')[6]) > 40)

spark.map(lambda i: i.upper())

spark.save_rdd_to_file("usa_latitude_greater_than_40", coalesce=1)
