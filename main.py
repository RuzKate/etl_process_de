import os
from dotenv import load_dotenv
from utils.extract import ExtractData
from utils.load import LoadDatabase
from utils.connect import DatabaseConnect
from utils.transform import TransformData
from utils.etl import Etl


def main():
    table_names = {
        'ft_balance_f': 'utf-8',
        'ft_posting_f': 'utf-8',
        'md_account_d': 'utf-8',
        'md_currency_d': 'mbcs',
        'md_exchange_rate_d': 'utf-8',
        'md_ledger_account_s': 'oem',
    }

    load_dotenv(".env")

    database_name = os.environ.get('DB_NAME')
    user_name = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')

    connection = DatabaseConnect(database_name, user_name, password, host, port)
    connection.connect_to_db()

    for table_name, code in table_names.items():
        extractor = ExtractData(table_name, code)
        transformer = TransformData()
        loader = LoadDatabase(table_name, connection, extractor)

        etl = Etl(extractor, transformer, loader)

        etl.etl_process(table_name, connection.engine)

if __name__ == '__main__':
    main()
    
