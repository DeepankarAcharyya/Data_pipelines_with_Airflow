""" Operator to perform data quality checks on the data loaded into the Redhsift cluster. """

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables = [],
                 redshift_conn_id = "redshift",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        # Mapping params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} returned no results".format(table))
                raise ValueError("Data quality check failed as {} returned no results".format(table))
            
            num_records = records[0][0]
            
            if num_records == 0:
                self.log.error("No records present in the table : {}".format(table))
                raise ValueError("No records present in the table : {}".format(table))
            
            self.log.info("Data quality on table {} check passed with {} records".format(table, num_records))