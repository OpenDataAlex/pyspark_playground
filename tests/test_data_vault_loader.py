"""
These are the tests for the data vault loader module.
"""

import datetime
import logging
import pandas as pd
from pyspark.sql import SparkSession
import pytz
import unittest


from data_vault_loader import DataVaultLoader


class DataVaultLoaderTests(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master('data_vault_loader_tests')
                .appName('data_vault_loader_testing')
                .enableHiveSupport()
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.timezone = pytz.timezone('UTC')

    def test_create_audit_fields(self):
        given_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                        'year': [2007, 2007, 2007]})

        given_result = self.spark.createDataFrame(given_result)

        expected_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                        'year': [2007, 2007, 2007],
                                        'create_actor_id': [1, 1, 1],
                                        'create_date_time': [datetime.now(tz=utc), datetime.now(tz=utc), datetime.now(tz=utc)],
                                        'create_process_id': [1, 1, 1],
                                        'create_source_id': [1, 1, 1]})

        given_result = DataVaultLoader().audit_field_manager(data_set=given_result, audit_type='create', process=1
                                                             , actor=1, source=1)
        given_result = given_result.toPandas()

        return self.assertEqual(given_result.columns, expected_result.columns)

    def test_universal_date_converter_with_timezone(self):
        """
        Testing that a date with a timezone gets successfully converted to UTC.
        :return:
        """
        orig_date = datetime.datetime.now(tz='EDT')

        expected_date = self.timezone.localize(orig_date)

        modified_date = DataVaultLoader().universal_date_converter(orig_date)

        return self.assertEqual(expected_date, modified_date)

    def test_universal_date_converter_without_timezone(self):
        """
        Testing that a date without a timezone gets successfully converted to UTC.
        :return:
        """
        orig_date = datetime.datetime.now()
        expected_date = self.timezone.localize(orig_date)

        modified_date = DataVaultLoader().universal_date_converter(orig_date)

        return self.assertEqual(expected_date, modified_date)
