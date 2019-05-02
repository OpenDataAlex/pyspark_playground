"""
These are the tests for the data vault loader module.
"""

import datetime
import dateutil
import logging
import pandas as pd
from pyspark.sql import SparkSession
import pytz
import time
import tzlocal
import unittest


from data_vault_loader import DataVaultLoader


class DataVaultLoaderTests(unittest.TestCase):
    # TODO:  Write unit test for validating audit fields without passing the audit date time.

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.sc = SparkSession.builder.master("local[2]")\
                            .appName(cls.__name__)\
                            .config("spark.executor.memory", "6gb")\
                            .enableHiveSupport()\
                            .getOrCreate()

        cls.timezone = pytz.timezone('UTC')
        cls.local_timezone = tzlocal.get_localzone()

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

    def test_audit_fields_create(self):
        """
        Verifying that the audit columns are being created for the 'create' option
        :return:
        """
        date_field = datetime.datetime.utcnow()
        given_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        given_result = self.sc.createDataFrame(given_result)

        expected_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                        'year': [2007, 2007, 2007],
                                        'create_actor_id': [1, 1, 1],
                                        'create_date_time': [date_field, date_field, date_field],
                                        'create_process_id': [1, 1, 1],
                                        'create_source_id': [1, 1, 1]})

        given_result = DataVaultLoader().audit_field_manager(data_set=given_result, audit_type='create', process=1
                                                             , actor=1, source=1, date=date_field)
        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result, check_dtype=False)

    def test_audit_fields_delete_type(self):
        """
        Verifying that the audit columns are being created for the 'delete' option
        :return:
        """
        date_field = datetime.datetime.utcnow()
        given_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        given_result = self.sc.createDataFrame(given_result)

        expected_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                        'year': [2007, 2007, 2007],
                                        'delete_actor_id': [1, 1, 1],
                                        'delete_date_time': [date_field, date_field, date_field],
                                        'delete_process_id': [1, 1, 1],
                                        'delete_source_id': [1, 1, 1]})

        given_result = DataVaultLoader().audit_field_manager(data_set=given_result, audit_type='delete', process=1
                                                             , actor=1, source=1, date=date_field)
        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result, check_dtype=False)

    def test_audit_fields_invalid_type(self):
        """
        Verifying that if invalid audit type is used, the fields will not be added to the data frame.
        :return:
        """
        given_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        given_result = self.sc.createDataFrame(given_result)

        with self.assertRaises(Exception) as context:

            DataVaultLoader().audit_field_manager(data_set=given_result, audit_type='blarg', process=1
                                                  , actor=1, source=1)

        return self.assertTrue('Invalid type provided.  Please use: create, update, delete' in str(context.exception))

    def test_audit_fields_update_type(self):
        """
        Verifying that the audit columns are being created for the 'update' option
        :return:
        """
        date_field = datetime.datetime.utcnow()
        given_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        given_result = self.sc.createDataFrame(given_result)

        expected_result = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                        'year': [2007, 2007, 2007],
                                        'update_actor_id': [1, 1, 1],
                                        'update_date_time': [date_field, date_field, date_field],
                                        'update_process_id': [1, 1, 1],
                                        'update_source_id': [1, 1, 1]})

        given_result = DataVaultLoader().audit_field_manager(data_set=given_result, audit_type='update', process=1
                                                             , actor=1, source=1, date=date_field)
        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result, check_dtype=False)

    def test_delta_all(self):
        """
        Verifying that if given a data set and an incremental with inserts, updates, and deletes, the result will have
        the correct keys and their delta_status.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['White Sox', 'Cardinals', 'Blue Jays', 'Cardinals'],
                                        'year': [2007, 2008, 2018, 2019]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Blue Jays', 'Red Sox', 'Cardinals'],
                                        'delta_status': ['I', 'D', 'U']})

        given_result = DataVaultLoader().get_delta(data_set=original_set, incremental_set=incremental_set
                                                   , key_field='team', filter_field='year', handle_delete=True
                                                   , handle_update=True)

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_delete(self):
        """
        Verifying that if a record that was in the original set is not in the delta set,
         it is passed with a delta_status of 'D'
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['White Sox', 'Cardinals'],
                                        'year': [2007, 2007]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Red Sox'],
                                        'delta_status': ['D']})

        given_result = DataVaultLoader().get_delta_delete(data_set=original_set, incremental_set=incremental_set
                                                          , key_field=['team'])

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_insert(self):
        """
        Verifying that if a record that was not in the original set, but is in the delta set, it is passed with a
         delta_status of 'I'.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals', 'Blue Jays'],
                                        'year': [2007, 2007, 2007, 2008]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Blue Jays'],
                                        'delta_status': ['I']})

        given_result = DataVaultLoader().get_delta_insert(data_set=original_set, incremental_set=incremental_set
                                                          , key_field=['team'])

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_insert_update(self):
        """
        Verifying that if given a data set and an incremental with inserts and updates, the result will have
        the correct keys and their delta_status. The test purposefully keeps the Red Sox delete to ensure that it is
        ignored.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['White Sox', 'Cardinals', 'Blue Jays'],
                                        'year': [2007, 2008, 2018]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Blue Jays', 'Cardinals'],
                                        'delta_status': ['I', 'U']})

        given_result = DataVaultLoader().get_delta(data_set=original_set, incremental_set=incremental_set
                                                   , key_field='team', filter_field='year', handle_delete=False
                                                   , handle_update=True)

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_insert_delete(self):
        """
        Verifying that if given a data set and an incremental with inserts and deletes, the result will have
        the correct keys and their delta_status. The test purposefully keeps the Cardinals update to ensure that it is
        ignored.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['White Sox', 'Cardinals', 'Blue Jays'],
                                        'year': [2007, 2008, 2018]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Blue Jays', 'Red Sox'],
                                        'delta_status': ['I', 'D']})

        given_result = DataVaultLoader().get_delta(data_set=original_set, incremental_set=incremental_set
                                                   , key_field='team', filter_field='year', handle_delete=True
                                                   , handle_update=False)

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_update_single(self):
        """
        Verifying that if a record that is in both sets but only has one change record , it is passed with a
         delta_status of 'I'.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                        'year': [2007, 2007, 2017]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Cardinals'],
                                        'delta_status': ['U']})

        given_result = DataVaultLoader().get_delta_update(data_set=original_set, incremental_set=incremental_set
                                                          , key_field=['team'], filter_field=['year'])

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_update_multiple(self):
        """
        Verifying that if a record that is in both sets but has multiple change records, the key will be passed
         with a delta_status of 'U'.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals', 'Cardinals'],
                                        'year': [2007, 2007, 2017, 2018]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Cardinals'],
                                        'delta_status': ['U']})

        given_result = DataVaultLoader().get_delta_update(data_set=original_set, incremental_set=incremental_set
                                                          , key_field=['team'], filter_field=['year'])

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_delta_update_multiple_revert(self):
        """
        Verifying that if a record that is in both sets but has multiple change records where the latest reverts back
        to what was in the original set, the key will still be passed with a delta_status of 'U'.
        :return:
        """
        original_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals'],
                                     'year': [2007, 2007, 2007]})

        incremental_set = pd.DataFrame({'team': ['Red Sox', 'White Sox', 'Cardinals', 'Cardinals'],
                                        'year': [2007, 2007, 2017, 2007]})

        original_set = self.sc.createDataFrame(original_set)
        incremental_set = self.sc.createDataFrame(incremental_set)

        expected_result = pd.DataFrame({'team': ['Cardinals'],
                                        'delta_status': ['U']})

        given_result = DataVaultLoader().get_delta_update(data_set=original_set, incremental_set=incremental_set
                                                          , key_field=['team'], filter_field=['year'])

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)

    def test_universal_date_converter_with_timezone(self):
        """
        Testing that a date with a timezone gets successfully converted to UTC.
        :return:
        """
        orig_date = datetime.datetime.now()
        expected_date = orig_date.astimezone(self.timezone)

        orig_date = self.local_timezone.localize(orig_date).strftime("%m/%d/%Y, %H:%M:%S.%f")

        modified_date = DataVaultLoader().universal_date_converter(date=orig_date)

        return self.assertEqual(expected_date, modified_date)

    def test_universal_date_converter_without_timezone(self):
        """
        Testing that a date without a timezone gets successfully converted to UTC.
        :return:
        """

        orig_date = datetime.datetime.now(pytz.timezone('America/New_York'))

        orig_date = orig_date.strftime("%m/%d/%Y, %H:%M:%S.%f")

        parsed_orig_date = dateutil.parser.parse(orig_date)
        expected_date = self.timezone.localize(parsed_orig_date) + datetime.timedelta(hours=4)

        modified_date = DataVaultLoader().universal_date_converter(orig_date, original_timezone='America/New_York')

        return self.assertEqual(expected_date, modified_date)

    def test_universal_identifier_generator(self):
        """
        Verify that when given a specific key, the UUID is created and appended to the data frame.
        :return:
        """

        original_set = pd.DataFrame({'team': ['Red Sox'],
                                     'year': [2007]})

        original_set = self.sc.createDataFrame(original_set)

        expected_result = pd.DataFrame({'team': ['Red Sox'],
                                        'year': [2007],
                                        'team_hash': ['ec1927fc85338c38a8d77a861d2ab8f7470720db'
                                                      'aa61f0071c0afbb597bb5ea5b77e0a8eda98f8f3'
                                                      '4e6469dae8ea26fcd4a7c1a47b38ed90ddb4ba355cc5ff7c']})

        given_result = DataVaultLoader().universal_identifier_generator(data_set=original_set, key_field='team'
                                                                        , key_name='team_hash')

        given_result = given_result.toPandas()

        return pd.testing.assert_frame_equal(given_result, expected_result)
