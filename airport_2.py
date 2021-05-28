from SparkBD import SparkBD

original = SparkBD("airport")

original.get_rdd("airports.csv")

# original.take(5, False)

prdd_city_country = original.get_copy().map(lambda x: (x.split(',')[2], x.split(',')[3]))

prdd_usa = prdd_city_country.get_copy().filter(lambda x: x[1] == 'United States')

prdd_usa.map_values(lambda x: x.upper())

# prdd_usa.take(5)

print(prdd_usa)

# prdd_usa.save_rdd_to_file('airport_usa', coalesce=1)
