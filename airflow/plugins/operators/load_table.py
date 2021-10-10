from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):
    """ Operator for loading fact and dimension tables
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 truncate=False,
                 *args, **kwargs):
        """ Constructor
        """
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        """ This method defines the main execution logic of the operator
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info('Delete existing data from table {} !!!'.format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Loading data in table {}'.format(self.table))
        redshift.run(self.sql_query)
