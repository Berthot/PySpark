from SparkBD import SparkBD

spark = SparkBD("airport")

spark.get_rdd("airports.csv")

spark.filter(lambda l: l.split(',')[3] == 'United States')

rdd_latitude_filtered = spark.filter(lambda l: float(l.split(',')[6]) > 40)

rdd_ex1 = rdd_latitude_filtered.map(lambda item: item.upper())

rdd_latitudes = spark.map(lambda l: float(l.split(',')[6]))

count = spark.count()

_sum = rdd_latitudes.reduce(lambda a, b: a + b)

print(f"Media das latitudes: {_sum.rdd / count}")

rdd_ex1.save_rdd_to_file("usa")
