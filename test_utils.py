import math
import unittest

from pyspark.sql import SparkSession

from utils import calc_location_entropy


class TestUtils(unittest.TestCase):
    def test_location_entropy(self):
        spark = SparkSession.builder.master("local[2]").appName("pyspark-sql-test").getOrCreate()
        lst = [('test-loc', 1)]
        df = spark.createDataFrame(lst, ['location', 'num_visits'])
        self.assertEquals(calc_location_entropy(df), 0)

        list2 = [('loc1', 1), ('loc2', 1)]
        df2 = spark.createDataFrame(list2, ['location', 'num_visits'])
        self.assertTrue(self.are_floats_equal(calc_location_entropy(df2), - 2 * 0.5 * math.log(0.5, 2)))

        list3 = [('loc1', 2), ('loc2', 2), ('loc3', 2)]
        one_third = 1.0 / 3
        df3 = spark.createDataFrame(list3, ['location', 'num_visits'])
        self.assertTrue(self.are_floats_equal(calc_location_entropy(df3), - 3 * one_third * math.log(one_third, 2)))

    @staticmethod
    def are_floats_equal(f1, f2, threshold=.0001):
        return abs(f1 - f2) <= threshold
