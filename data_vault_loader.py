# Purpose:  To build a loading library for the various components of data vault utilizing Spark.
# Initial creation 23/04/2019

from datetime import datetime
from dateutil import parser

from pyspark import SparkContext
import pyspark.sql.functions as F
from pytz import timezone, utc


class DataVaultLoader():

    @staticmethod
    def audit_field_manager(data_set, audit_type, process, actor, source):
        """
        Add audit metadata to datasets.  This is intended for internal steps (i.e. into staging, into warehouse
        , into presentation, etc.)
        :param audit_type: The type of auditing needing recorded.
        :type audit_type: String   Valid types:  create, update, delete
        :param process: The process identifier processing the data.
        :type process: String
        :param actor: The tool or person kicking off the process.
        :type actor: String
        :param source:  The source identifier where the data is coming from.
        :type source: String
        :return audit_fields: Dictionary of audit fields.
        """
        valid_audit_types = ['create', 'update', 'delete']
        audit_type = audit_type.lower()

        actor_field = actor
        date_field = datetime.now(tz=utc)
        process_field = process
        source_field = source

        if audit_type in valid_audit_types:
            data_set = data_set.withColumn(audit_type + '_actor_id', F.lit(actor_field))
            data_set = data_set.withColumn(audit_type + '_date_time', F.lit(date_field))
            data_set = data_set.withColumn(audit_type + '_process_id', F.lit(process_field))
            data_set = data_set.withColumn(audit_type + '_source_id', F.lit(source_field))

        else:
            raise Exception('Invalid type provided.  Please use: ' + valid_audit_types)
            exit()

        return data_set

    @staticmethod
    def universal_date_converter(date, day_first=False, year_first=False, original_timezone=None, time_zone='UTC'):

        """
        Universal date converter converts the provided date into a UTC timestamp.
        :param date: The date field to be converted.
        :type date: date string
        :param day_first: To protect against ambiguous formats, does the day come before the month?
        :type day_first: Boolean
        :param year_first: To protect against ambiguous formats, does the year come first?
        :type year_first: Boolean
        :param original_timezone:  Provide the date's timezone if the date's timezone is known but not part of the date.
          Refer to the Olsen tz database for valid string names. Not set assumes the date has timezone.
        :type original_timezone: string
        :param time_zone:  The timezone that the date needs to be converted to.  Refer to the Olsen tz database for
         valid string names.  Default is UTC
        :return converted_date: Converted date string
        """

        orig_date = parser(date, dayfirst=day_first, yearfirst=year_first)

        if original_timezone:
            orig_date = timezone(original_timezone).localize(orig_date)

        converted_date = orig_date.astimezone(time_zone)

        return converted_date

    @staticmethod
    def universal_identifier_generator(data_set, key_field, key_name):
        """
        Universal Identifier Generator generates UUIDs based on data fields from the data set.  This is the equivalent
        of a validation hash, based on business key(s).
        :param key_field:  Business key field(s) to be hashed.
        :type key_field: string or list
        :param key_name: Name of the uuid field
        :type key_name: String
        :return uuid_key:
        """

        if type(key_field) is not list:
            key_field = [key_field]

        data_set = data_set.withColumn(key_name, F.sha2(F.concat_ws('||', *key_field), 512))

        return data_set

    @staticmethod
    def get_delta(data_set, incremental_set, key_field, filter_field, handle_update=False, handle_delete=False):
        """
        Provided an incremental data set and filter field(s), find all data that has changed.
        :param incremental_set: The data set's provided update.
        :param key_field:  The field(s) used to match records by
        :type key_field: str for one, list for multiple
        :param filter_field: What fields need to be used for comparison?
        :type filter_field: str for one, list for multiple
        :param handle_update: Do updated records need to be returned?
        :type handle_update: Boolean
        :param handle_delete: Do deleted records need to be returned?
        :type handle_delete: Boolean
        :return:
        """
        if type(filter_field) is not list:
            filter_field = [filter_field]

        if type(key_field) is not list:
            key_field = [key_field]

        delta_data = self.get_delta_insert(incremental_set=incremental_set, key_field=key_field)

        if handle_delete:
            delete_records = self.get_delta_delete(incremental_set=incremental_set, filter_field=filter_field)

            delta_data = SparkContext.union(delta_data, delete_records)

        if handle_update:
            update_records = self.get_delta_update(incremental_set=incremental_set, key_field=key_field, filter_field=filter_field)

            delta_data = SparkContext.union(delta_data, update_records)

        return delta_data

    @staticmethod
    def get_delta_delete(data_set, incremental_set, filter_field):
        """
        Find all deleted records in delta set
        :param incremental_set: The primary data set's updated set
        :param filter_field: The list of fields used for the comparison.
        :return:
        """

        current_set = data_set.select(*filter_field)

        delete_records = current_set.subtract(incremental_set.select(*filter_field))
        delete_records = delete_records.withColumn('delta_status', F.lit('D'))

        current_set.unpersist()

        return delete_records

    @staticmethod
    def get_delta_insert(data_set, incremental_set, key_field):
        """
        Find all new records in delta set
        :param incremental_set:
        :param key_field: The list of fields used to match records.
        :return:
        """

        current_set = data_set.select(*filter_field)

        new_records = incremental_set.select(*filter_field).subtract(current_set)
        new_records = new_records.withColumn('delta_status', F.lit('I'))

        current_set.unpersist()

        return new_records

    @staticmethod
    def get_delta_update(data_set, incremental_set, key_field, filter_field):
        """
        Find all updated records in a delta set.
        :param data_set:  The primary data set.
        :param incremental_set: The primary data set's updated set
        :param key_field:  The list of fields used to match records.
        :param filter_field: The list of fields used for the comparison.  The filter(s) are for NOT matching.
        :return:
        """
        # TODO: How to handle multiple changes in single set.  Have to handle the change and change back edge case.
        # Could include create date?

        # First need to find all records that are in both the original and delta sets
        original_set = data_set.alias('original_set')
        delta_set = incremental_set.alias('delta_set')

        key_match = F.reduce(
            lambda a,b: (a | b),
            [original_set[col] == delta_set[col]
             for col in key_field
             ]
        )

        comparison_query = F.reduce(
            lambda a, b: (a | b),
            [original_sat[col] != delta_set[col]
             for col in filter_field
             ]
        )

        update_set = original_set.join(delta_set, on=key_match & comparison_query)

        update_set = update_set.withColumn('delta_status', F.lit('U'))

        return update_set
