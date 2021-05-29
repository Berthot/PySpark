import shutil
from functools import reduce
from typing import Callable, Any, List

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pathlib import Path


class SparkBD:

    def __init__(self, project_name):
        self._OUT_DIR = Path("output_dir")
        self._project_name = project_name
        self.context = None
        self._setup()
        self._setup_samples()
        self.rdd = None

    def _get_copy_name(self, file_name):
        if file_name == '':
            return f"{self._project_name}_copy"
        return file_name

    def get_copy(self, file_name=''):
        spark = SparkBD(self._get_copy_name(file_name))
        spark.rdd = self.rdd
        spark.context = self.context
        return spark

    def get_rdd(self, file_name: str):
        self.rdd = self._context.textFile(f'file://{SparkFiles.get(file_name)}')

    def get_rdd_by_tuple(self, tuples):
        self.rdd = self._context.parallelize(tuples)

    def flatmap(self, *args):
        self.rdd = self.rdd.flatMap(*args)

    def filter(self, *args, copy_name=''):
        self.rdd = self.rdd.filter(*args)
        return self.get_copy(copy_name)

    def map(self, *args, copy_name=''):
        self.rdd = self.rdd.map(*args)
        return self.get_copy(copy_name)

    def map_values(self, *args, copy_name=''):
        self.rdd = self.rdd.mapValues(*args)
        return self.get_copy(copy_name)

    def sort_by_key(self, *args, copy_name='', ascending=False):
        self.rdd = self.rdd.sortByKey(*args, ascending=ascending)
        return self.get_copy(copy_name)

    def sort_by(self, *args, copy_name='', ascending=False):
        self.rdd = self.rdd.sortBy(*args, ascending=ascending)
        return self.get_copy(copy_name)

    def take(self, count: int, active: bool = True):
        if active:
            self.rdd = self.rdd.take(count)

    def reduce(self, *args, copy_name=''):
        self.rdd = self.rdd.reduce(*args)
        return self.get_copy(copy_name)

    def reduce_by_key(self, *args, copy_name=''):
        """
        reduce and keep the same key
        :param args:
        :param copy_name:
        :return:
        """
        self.rdd = self.rdd.reduceByKey(*args)
        return self.get_copy(copy_name)

    def count_by_value(self):
        self.rdd = self.rdd.countByValue()

    def get_items(self):
        return self.rdd.items()

    def __str__(self):
        acc = ''
        for value in self.rdd:
            acc += str(value) + '\n'
        return acc

    @property
    def _files(self):
        return ['airports.csv',
                'bible.txt',
                'RealEstate.csv',
                '2016-stack-overflow-survey-responses.csv',
                'uk-postcode.csv',
                'uk-makerspaces-identifiable-data.csv']

    @property
    def _base_url(self):
        return 'https://www.ppgia.pucpr.br/~jean.barddal/bigdata/'

    def _setup(self):
        spark = SparkSession.builder.appName(self._project_name).getOrCreate()
        self._context = spark.sparkContext
        self._context.setLogLevel("OFF")

    def _setup_samples(self):
        for url in [self._base_url + x for x in self._files]:
            self._context.addFile(url)

    def save_rdd_to_file(self, file_name="", coalesce=2):
        name = self._project_name if file_name == "" else file_name
        self.rdd = self.rdd.coalesce(coalesce)
        file = self._OUT_DIR / name
        if file.exists():
            shutil.rmtree(self._OUT_DIR)
        self.rdd.saveAsTextFile(str(file))

    def count(self):
        return self.rdd.count()
