import pandas as pd

class ExtractData:
    def __init__(self, table_name, code):
        self.path_file = f'csv_file/{table_name}.csv'
        self.code = code
        self.data = None


    def extract_data(self):
        """Извлечение данных

            :param path_file: путь к файлу
            :type path_file: str

            :param code: кодировка файла
            :type code: str

            :rtype: DataFrame
            :return: извлеченные данные из csv файла
        """
        self.data = pd.read_csv(self.path_file, sep=';', encoding=self.code, na_filter=False) 