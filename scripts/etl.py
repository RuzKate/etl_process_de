from datetime import datetime
import time
import json
from sqlalchemy.sql import text
from scripts.extract import ExtractData
from scripts.transform import TransformData
from scripts.load import LoadDatabase

class Etl:
    def __init__(self, extractor: ExtractData, transformer: TransformData, loader: LoadDatabase):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def etl_process(self, table_name, engine):
        start_time = datetime.now()

        with engine.connect() as conn:      
            try:
                self.extractor.extract_data()
            except Exception as error:
                json_str = json.dumps({"error_message": str(error)})
                conn.execute(text(f"""
                            CALL logs.insert_etl_logs(
                                '{start_time}',
                                'Error while extracting data from csv file {table_name}.csv',
                                'ERROR',
                                $${json_str}$$)
                            """))
                conn.commit()
                print(error)
                return

            try:    
                self.transformer.transform_data(self.extractor.data, table_name)
            except Exception as error:
                json_str = json.dumps({"error_message": str(error)})
                conn.execute(text(f"""
                            CALL logs.insert_etl_logs(
                                '{start_time}',
                                'Error while processing data from file {table_name}.csv',
                                'ERROR',
                                $${json_str}$$)
                            """))
                conn.commit()
                print(error)
                return
        
            try:
                self.loader.load_to_db()
            except Exception as error:
                json_str = json.dumps({"error_message": str(error)})
                conn.execute(text(f"""
                            CALL logs.insert_etl_logs(
                                '{start_time}',
                                'Error loading data into database {table_name} table',
                                'ERROR',
                                $${json_str}$$)
                            """))
                conn.commit()
                print(error)
                return
            
            time.sleep(5)
            conn.execute(text(f"""
                        CALL logs.insert_etl_logs(
                            '{start_time}',
                            'ETL process completed successfully',
                            'INTO',
                            null)
                        """))
            conn.commit()                