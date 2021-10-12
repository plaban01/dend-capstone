from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ Operator for performing data quality checks
    """
    ui_color = '#89DA59'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 checks,
                 *args, **kwargs):
        """ Constructor 
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks
        
    def format_errors(self, checks):
        """ This method will format the failed checks for displaying in the output
        
        INPUTS:
            checks: Array of checks 
                [ { 'name': 'Name of check', 'expected': Expected value, 'actual': Actual value } ]
                
        Returns newline separated string describing failed checks
        
        """
        return "\n".join(
            list(
                map(
                    lambda check: "{} check failed. Expected {}, actual {}".format(
                        check['name'], check['expected'], check['actual']
                    ),
                    checks
                )
            )
        )

    def execute(self, context):
        """ This method defines the main execution logic of the operator
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        failed_checks = []
        for check in self.checks:
            sql = check['sql'].format(**context)
            self.log.info(f"Running query {sql}")
            output = redshift.get_records(sql)[0]
            
            if not eval(check['condition'], dict(result=output[0])):
                failed_checks.append({
                    'name': check['name'],
                    'expected': check['expected'],
                    'actual': output[0]
                })
                
        if failed_checks:
            self.log.info('Data quality check failed !!!')
            self.log.info(self.format_errors(failed_checks))
            raise ValueError('Data quality check failed !!!')