from typing import Tuple

from SparkBD import SparkBD

original = SparkBD("test")

tuples = [('test', 1), ('test', 1), ('test2', 2)]

original.get_rdd_by_tuple(tuples)

original.save_rdd_to_file("test", coalesce=1)
