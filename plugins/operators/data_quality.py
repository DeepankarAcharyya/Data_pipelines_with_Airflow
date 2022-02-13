from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from numpy import record

class DataQualityOperator(BaseOperator):
    """ Operator to perform data quality checks on the data loaded into the Redhsift cluster. """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dataquality_checks = [],
                 redshift_conn_id = "redshift",
                 *args, **kwargs):

        """
        Operator to perform data quality checks on the data loaded into the Redhsift cluster.

        Args:
            dataquality_checks: The list of quality checks that are required to be performed
            redshift_conn_id: Reference to the redshift credentials
        
        """
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        # Mapping params
        self.redshift_conn_id = redshift_conn_id
        self.dataquality_checks = dataquality_checks

    def execute(self, context):
        if len(self.dataquality_checks) <= 0:
            self.log.info("No data quality checks provided")
            return

        errors = 0
        tests_failed = []
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.dataquality_checks:
            sql_query = check.get('sql')
            exp_result = check.get('expected_result')

            try:
                self.log.info(f"Running query: {sql_query}")
                records = redshift_hook.get_records(sql_query)[0][0]
            except Exception as e:
                self.log.info(f"Query failed : {e}")
            
            if exp_result != records:
                errors += 1
                tests_failed.append(sql_query)
            
            if errors>0:
                self.log.info('Tests failed')
                self.log.info(tests_failed)
                raise ValueError('Data quality check(s) failed')
            else:
                self.log.info("All data quality checks passed")