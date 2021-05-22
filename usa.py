from SparkBD import SparkBD

original = SparkBD("airport")

original.get_rdd("airports.csv")

original.filter(lambda l: l.split(',')[3] == 'United States')

rdd_latitude_filtered = original.filter(lambda l: float(l.split(',')[6]) > 40)

rdd_ex1 = rdd_latitude_filtered.map(lambda item: item.upper())

rdd_latitudes = original.map(lambda l: float(l.split(',')[6]))

count = original.count()

_sum = rdd_latitudes.reduce(lambda a, b: a + b)

print(f"Media das latitudes: {_sum.rdd / count}")

rdd_ex1.save_rdd_to_file("usa")
