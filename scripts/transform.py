class TransformData:

    def transform_data(self, data, table_name):
        """Трансформация данных

            :param data: извлеченные данные из csv файла
            :type data: DataFrame

            :param table_name: имя файла
            :type table_name: str
        """
        data.rename(columns=lambda x: x.lower(), inplace=True)
        data.drop(data.columns[[0]], axis=1 , inplace=True)

        if table_name == 'md_currency_d':
            data.loc[data['currency_code'] == '999', 'code_iso_char'] = 'XXX'