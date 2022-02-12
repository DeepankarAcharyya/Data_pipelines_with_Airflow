from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ Operator to load data into a fact table. """

    ui_color = '#F98866'

    TRUNCATE_TABLE_SQL = """
        TRUNCATE TABLE {};
        """
    
    INSERT_DATA_SQL = """
        INSERT INTO {} {};
        """

    @apply_defaults
    def __init__(self,
                 table_name,
                 sql,
                 truncate_table=False,
                 redshift_conn_id = "redshift",
                 *args, **kwargs):

        """
         Operator to load data into a fact table.

         Args:
            table_name: Name of the fact table
            sql: the sql query to load data into the table
            truncate_table: Whether to truncate the table
            redshift_conn_id: Reference to the redshift credentials
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        # Mapping params
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql = sql
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_table:
            self.log.info('Truncating table {self.table_name}')
            redshift_hook.run(self.TRUNCATE_TABLE_SQL.format(self.table_name))

        self.log.info('Inserting data into fact table : {self.table_name}')
        redshift_hook.run(self.INSERT_DATA_SQL.format(
            self.table_name,
            self.sql
        ))