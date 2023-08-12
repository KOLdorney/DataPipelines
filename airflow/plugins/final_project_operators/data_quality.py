from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_tests = sql_tests
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        
        for test in self.sql_tests:
            result = redshift.get_records(test['check_sql'])
            actual_result = result[0][0]
            expected_result = test['expected_result']
            if actual_result != expected_result:
                raise ValueError("Data quality check failed.")
        self.log.info('DataQualityOperator successful')
