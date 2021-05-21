import shutil
from functools import reduce

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
        self.content = None

    def get_rdd(self, file_name: str):
        self.rdd = self._context.textFile(f'file://{SparkFiles.get(file_name)}')

    def flatmap(self, *args):
        self.rdd = self.rdd.flatMap(*args)

    def filter(self, *args):
        self.rdd = self.rdd.filter(*args)

    def reduce(self, *args):
        self.rdd = self.rdd.reduce(*args)

    def count_by_value(self):
        self.rdd = self.rdd.countByValue()

    def get_items(self):
        return self.rdd.items()

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
        return 'http://www.ppgia.pucpr.br/~jean.barddal/bigdata/'

    def _setup(self, ):
        spark = SparkSession.builder.appName(self._project_name).getOrCreate()
        self._context = spark.sparkContext

    def _setup_samples(self):
        for url in [self._base_url + x for x in self._files]:
            self._context.addFile(url)

    def save_rdd_to_file(self, file_name="testando"):
        name = self._project_name if file_name == "" else file_name
        file = self._OUT_DIR / name
        if file.exists():
            shutil.rmtree(self._OUT_DIR)
        self.rdd.saveAsTextFile(str(file))
