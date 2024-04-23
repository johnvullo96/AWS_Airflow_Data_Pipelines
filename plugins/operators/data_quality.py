from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
    def execute(self, context):
        self.log.info('Data quality check started...') 
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)    
        for table in self.tables:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')    
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] == 0:
                self.log.error( f'Data quality check failure: table "{table}" is empty')
                raise ValueError( f'Data quality check failure: table "{table}" is empty')
            self.log.info(f'Data quality check on table "{table}" passed with {records[0][0]} records')
