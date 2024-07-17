import pandas as pd

class ExtractData:
    def __init__(self, table_name, code):
        """Инициализатор класса
        
            :param table_name: название таблицы
            :type table_name: str

            :param code: кодировка CSV-файла
            :type code: str
        """
        self.path_file = f'csv_file/{table_name}.csv'
        self.code = code
        self.data = None


    def extract_data(self):
        """Извлечение данных из CSV-файла"""
        self.data = pd.read_csv(self.path_file, sep=';', encoding=self.code, na_filter=False) 
