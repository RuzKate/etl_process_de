from sqlalchemy import inspect
from sqlalchemy.sql import text
from sqlalchemy.exc import IntegrityError
from scripts.connect import DatabaseConnect
from scripts.extract import ExtractData

class LoadDatabase:
    def __init__(self, table_name, connection: DatabaseConnect, extractor: ExtractData):
        self.table_name = table_name
        self.engine = connection.engine
        self.extractor = extractor

    def load_to_db(self):
        """Загрузка данных в базу данных

            :param data: извлеченные данные из csv файла
            :type data: DataFrame

            :param table_name: имя файла
            :type table_name: str

            :param engine: движок, отвечающий за взаимодействие с базой данных
            :type engine: объект класса Engine
        """
        inspector = inspect(self.engine)
        primary_key_columns = inspector.get_pk_constraint(table_name=self.table_name, schema='ds')['constrained_columns']
        
        data_dict = self.extractor.data.to_dict(orient='records')

        columns = ', '.join(f':{column}' for column in list(self.extractor.data))

        sql_query = text(f"""
                INSERT INTO ds.{self.table_name} ({', '.join(list(self.extractor.data))})
                VALUES ({columns}) 
                ON CONFLICT ({', '.join(primary_key_columns)}) 
                DO UPDATE SET ({', '.join(list(self.extractor.data))}) = ({columns})
            """)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(sql_query, data_dict)
                conn.commit()
        except IntegrityError as error:
            print(f'Error adding/updating records: {error}')