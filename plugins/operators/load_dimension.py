""" Operator to load data into a dimension table. """

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    TRUNCATE_TABLE_SQL = """
        TRUNCATE TABLE {};
        """
    
    INSERT_DATA_SQL = """
        INSERT INTO {} ({}) {};
        """

    @apply_defaults
    def __init__(self,
                 table_name,
                 insert_columns,
                 data,
                 truncate_table=False,
                 redshift_conn_id = "redshift",
                 *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        # Mapping params
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.data = data
        self.insert_columns = insert_columns
        self.truncate_table = truncate_table

        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_table:
            self.log.info('Truncating table {self.table_name}')
            redshift_hook.run(self.TRUNCATE_TABLE_SQL.format(self.table_name))

        self.log.info('Inserting data into dimension table : {self.table_name}')
        redshift_hook.run(self.INSERT_DATA_SQL.format(
            self.table_name,
            self.insert_columns,
            self.data
        ))
