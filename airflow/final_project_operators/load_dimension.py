from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 trunc=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.trunc = trunc

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connected to redshift")
        if self.trunc:
            trunc_sql = f"TRUNCATE {self.table}"
            redshift.run(trunc_sql)
        insert_sql = f"INSERT INTO {self.table} ({self.sql})"
        redshift.run(insert_sql)
        self.log.info('LoadDimensionOperator successful')
