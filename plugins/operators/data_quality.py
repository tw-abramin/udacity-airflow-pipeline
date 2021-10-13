from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=[],
                 extra_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.extra_checks=extra_checks
    
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")

        if len(self.extra_checks) > 0:
            for check in self.extra_checks:
                result = redshift_hook.run(check['sql'])
                if result != check['expected']:
                    raise ValueError(f"Data quality check failed. {check['error_message']}")
