import math

import pyspark.sql.functions as funcs
from pyspark.sql.types import *


def calc_location_entropy(df):
    """
    :param df: A Spark dataframe with 2 columns available: location and num_vists
    :return: The location entropy of the dataframe
    """
    assert df is not None, 'DataFrame should not be null'
    assert 'location' in df.columns, 'Location is not present in the dataframe'
    assert 'num_visits' in df.columns, 'Number of visits is not present in the dataframe'

    def single_entropy(visits, total_visits):
        p = 1. * visits / total_visits
        return p * math.log(p, 2)

    single_entropy_udf = funcs.udf(single_entropy, DoubleType())

    total_num_visits = df.select(funcs.sum('num_visits').alias('total_visits')).first()['total_visits']
    total_entropy = df.groupBy('location').agg(funcs.sum('num_visits').alias('num_visits')) \
        .withColumn('total_num_visits', funcs.lit(total_num_visits).cast(LongType())) \
        .withColumn('entropy', single_entropy_udf('num_visits', 'total_num_visits')) \
        .select(funcs.sum('entropy').alias('total_entropy')) \
        .first()['total_entropy']

    return -total_entropy
