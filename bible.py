from SparkBD import SparkBD

spark = SparkBD("bible")

rdd = spark.get_rdd("bible.txt")

spark.flatmap(lambda x: x.split(" "))

spark.filter(lambda x: x != '')

spark.save_rdd_to_file()

spark.count_by_value()

for k, v in spark.get_items():
    print(f'{k}: {v}')

