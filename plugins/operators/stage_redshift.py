from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """ Operator to load data from the json log files into the staging tables in the redshift cluster."""

    ui_color = '#358140'

    TRUNCATE_TABLE_SQL = """
        TRUNCATE TABLE {};
        """

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
        TRUNCATECOLUMNS 
        BLANKSASNULL 
        EMPTYASNULL;
    """

    @apply_defaults
    def __init__(self,
                 table,
                 s3_path,
                 aws_conn_id,
                 extra_params="",
                 redshift_conn_id="redshift",
                 region="us-west-2",
                 truncate_table=True,
                 *args, **kwargs):
        
        """
        Operator to load data from the json log files into the staging tables in the redshift cluster.

        Args:
            table: The table name
            s3_path: Path to the log files in s3
            aws_conn_id: Reference to the aws credentials
            extra_params: Extra flags that might be required in the COPY cmd
            redshift_conn_id: Reference to the redshift credentials
            region: Region in which the S3 bucket is present 
            truncate_table: Whether to truncate the table
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        # Mapping the params here
        self.table_name = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_path = s3_path
        self.region = region
        self.truncate_table = truncate_table
        self.extra_params = extra_params
       

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_table:
            self.log.info('Truncating table {self.table_name}')
            redshift_hook.run(self.TRUNCATE_TABLE_SQL.format(self.table_name))

        self.log.info("Copying data from S3 to Redshift: STARTING")
        
        sql_statement = self.COPY_SQL.format(
            self.table_name, 
            self.s3_path, 
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.region,
            self.extra_params
            )

        redshift_hook.run(sql_statement)
        self.log.info("Copying data from S3 to Redshift: DONE")

