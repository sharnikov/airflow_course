from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class PostgreSQLCountRows(BaseOperator):

    def __init__(self,
                 table_name,
                 schema,
                 *args,
                 **kwargs):
        self.table_name = table_name
        self.hook = PostgresHook()
        self.schema = schema
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        count = self.hook.get_records(sql=f"SELECT COUNT(*) FROM {self.table_name};")
        print(f"Amount of rows in {self.table_name} is {count}")
