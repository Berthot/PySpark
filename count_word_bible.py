from SparkBD import SparkBD

original = SparkBD("bible_word_count")

rdd = original.get_rdd("bible.txt")

original.flatmap(lambda x: x.split(" "))

original.filter(lambda x: x != '')

prdd_word_count = original.get_copy().map(lambda x: (x, 1))

prdd_word_count.reduce_by_key(lambda x, y: x + y)

prdd_word_count.save_rdd_to_file("bible_word_count", coalesce=1)

#
# for k, v in original.get_items():
#     print(f'{k}: {v}')
