from types import NoneType
import pyodbc
import json
import json
from time import sleep
from os import system
import platform
from typing import Union
import re

replace_values = [
    '"', "'", '?', '/', '\\', '(', ')', '[', ']', '{', '}', '+', '*', '&',
    '^', '%', '$', '#', '@', '!', '~', '`', '|', '<', '>', '=', ':', ';',
    '  ', ',', ' ', '-', '.', 'ç'
]

class DataBase():
    def __init__(self, config:dict) -> None:
        self.__server = config['server']
        self.__database = config['database']
        self.__username = config['username']
        self.__password = config['password']
        self.__driver = config['driver']
        self.cursor = self.connect()

    def get_database_config(self):
        return {
            "server": self.__server,
            "database": self.__database,
            "username": self.__username,
            "password": self.__password,
            "driver": self.__driver
        }
    
    def connect(self):
        db_config = self.get_database_config()
        connection_data = f"SERVER={db_config['server']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']};DRIVER={{{db_config['driver']}}};"
        
        conn = pyodbc.connect(connection_data)
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setencoding(encoding='utf-8')

        cursor = conn.cursor()
        return cursor

    def close(self):
        self.cursor.close()
        self.cursor.close()

    def _drop_table(self, table_name):
        self.cursor.execute(f"""DROP TABLE IF EXISTS {table_name};""")
        self.cursor.commit()

    def _drop_all_tables(self):
        # Primeiro, buscar todos os nomes das tabelas no esquema público
        self.cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public';
        """)
        
        tables = self.cursor.fetchall()
        
        # Apagar cada tabela individualmente
        for table in tables:
            table_name = table[0]
            drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            self.cursor.execute(drop_query)
            print(f"Tabela {table_name} apagada com sucesso.")

        # Confirma as alterações no banco de dados
        self.cursor.commit()

    def _erase_table(self, table_name):
        self.cursor.execute(f"""DELETE FROM {table_name};""")
        self.cursor.commit()

    def _create_table_if_not_exists(self, table_name, params):
        check_query = f"SELECT to_regclass('public.{table_name}');"
        self.cursor.execute(check_query)
        result = self.cursor.fetchone()

        # Se a tabela não existir, result será None
        if result and result[0] is not None:
            table_exists = True
        else:
            table_exists = False

        keys = []
        # Se a tabela não existir, crie-a
        if not table_exists:
            query_arg = f"""CREATE TABLE {replace_to_postgres_name(table_name)} (
                            id SERIAL PRIMARY KEY, """
            for key, value in params.items():
                if key == list(params.keys())[-1]:
                    if replace_to_postgres_name(key) not in keys:
                        keys.append(replace_to_postgres_name(key))
                        query_arg += f'{replace_to_postgres_name(key)} {self.python_to_postgres_type(value)}'
                    else:
                        query_arg += f'{replace_to_postgres_name(key)}_2 {self.python_to_postgres_type(value)}'
                else:
                    if replace_to_postgres_name(key) not in keys:
                        keys.append(replace_to_postgres_name(key))
                        query_arg += f'{replace_to_postgres_name(key)} {self.python_to_postgres_type(value)}, '
                    else:
                        query_arg += f'{replace_to_postgres_name(key)}_2 {self.python_to_postgres_type(value)}, '
            query_arg += ");"
            print(query_arg)
            self.cursor.execute(query_arg)
            self.cursor.commit()

            print(f"Tabela '{table_name}' criada com sucesso.")
        else:
            print(f"A tabela '{table_name}' já existe.")

    def select_table(self):
        self.cursor.execute("""SELECT * FROM clientes_aniversariantes;""")
        return self.cursor.fetchall()

    def insert_in_table(self, table_name, row:dict, errors:int):
        # query = f"""
        # INSERT INTO {table_name} (name, description, url, tags) 
        # VALUES (?, ?, ?, ?);
        # """

        def add_quotation_marks_in_string(data):
            if isinstance(data, NoneType):
                return 'NULL'
            elif isinstance(data, str) and data.strip() == '':
                return 'NULL'
            elif isinstance(data, str):
                query = f"'{data}'"
            elif isinstance(data, int) or isinstance(data, float):
                return str(data)
            else:
                query = f"'{data}'"
            return query

        
        # Inicializa as partes da query
        key_args = []
        values_args = []

        # Itera sobre o dicionário e formata as chaves e valores
        for key, value in row.items():
            key_args.append(replace_to_postgres_name(key))
            data = data_type_manager(key, value)
            values_args.append(add_quotation_marks_in_string(data))

        # already_exists= []
        # Cria a query concatenando as chaves e valores
        # for key in key_args:
            # if key not in already_exists:
                # already_exists.append(key)
            # else:
                # key_args[key] = key + '_2'
        if len(key_args) != len(set(key_args)):
            already_exists = set()

            for i, key in enumerate(key_args):
                if key not in already_exists:
                    already_exists.add(key)
                else:
                    new_key = key + '_2'
                    while new_key in already_exists:
                        # Incrementa o sufixo até encontrar uma chave única
                        new_key = new_key[:-1] + str(int(new_key[-1]) + 1)
                    already_exists.add(new_key)
                    key_args[i] = new_key
        keys = ', '.join(key_args)
        values = ', '.join(values_args)
        query_arg = f"""INSERT INTO {replace_to_postgres_name(table_name)} ({keys}) VALUES ({values});"""

        print(query_arg)  # Para depuração
        try:
            # Executa a query
            self.cursor.execute(query_arg)
            self.cursor.commit()
        except Exception as e:
            errors = 1
            print("\n\nError executing SQL command\n\n")
            print(e)
            # exit()
            return errors
        return 0

    def python_to_postgres_type(self, arg):
        type_mapping = {
            str: "TEXT",
            int: "INTEGER",
            float: "REAL",
            bool: "BOOLEAN",
            list: "ARRAY",
            dict: "JSONB",
            tuple: "TEXT",  # Não existe um equivalente direto para tuple, mas pode ser tratado como TEXT ou ARRAY
            bytes: "BYTEA",
            None: "NULL",
        }

        return type_mapping.get(type(arg), "TEXT")

def replace_to_postgres_name(name):
    # Remove ou substitui caracteres indesejados
    name = (name.replace('"', '')
                .replace("'", '')
                .replace('?', '')
                .replace('/', ' ')
                .replace('\\', '')
                .replace('(', '')
                .replace(')', '')
                .replace('[', '')
                .replace(']', '')
                .replace('{', '')
                .replace('}', '')
                .replace('+', '_')
                .replace('*', '_')
                .replace('&', '_')
                .replace('^', '_')
                .replace('%', '')
                .replace('$', '')
                .replace('#', '')
                .replace('@', '')
                .replace('!', '')
                .replace('~', '')
                .replace('`', '')
                .replace('|', '_')
                .replace('<', '')
                .replace('>', '')
                .replace('=', '_')
                .replace(':', '')
                .replace(';', '')
                .replace('  ', ' ')
                .replace(',', ' ')
                .replace(' ', '_')
                .replace('-', '_')
                .replace('.', '')
                .replace('ç', 'c')
                .lower())

    if name == 'id':
        return 'id_avec'
    
    if name[-1] in ['.', '%', '_', '-']:
        name = name[:-1]
    
    if name[0] in ['.', '%', '_', '-']:
        name = name[1:]
    
    # Verifica se a string contém apenas caracteres válidos (letras, números e underscores)
    # name = re.sub(r'[^a-z0-9_]', '', name)
    
    return name

def is_cpf(key, cpf):
    if key.strip().lower() == 'cpf':
        return True
    # Remove caracteres não numéricos
    cpf_numbers = re.sub(r'\D', '', cpf)
    
    # Verifica se o CPF tem 11 dígitos
    if len(cpf_numbers) != 11:
        return False
    
    # Verifica se todos os dígitos são iguais (caso de CPF inválido)
    if cpf_numbers == cpf_numbers[0] * 11:
        return False

    if '-' not in cpf or '.' not in cpf:
        return False

    return True

def is_cep(key:str ,cep:str) -> bool:
    if key.strip().lower() == 'cep':
        return True
    cep_pattern = re.compile(r'^\d{5}-\d{3}$')
    
    if cep_pattern.match(cep):
        return True
    else:
        return False

def data_type_manager(key, value):
    if is_number(value):
        if isinstance(value, Union[int, float]):
            return value
        if value.startswith('-'):
            return replace_number_type(key, value)
        elif ',' in value and value.endswith(',') == False and value.startswith(',') == False:
            return replace_number_type(key, value)
        if is_phone_number(key, value):
            # print('phone: ', value)
            return f'{value.replace(" ","")}'
        elif is_cep(key, value):
            # print('cep: ', value)
            return f'{value}'
        elif is_cpf(key, value):
            # print('cpf: ', value)
            return f'{value}'
        else:
            # print('number: ', value)
            # system('clear')
            return replace_number_type(key, value)
    else:
        # print('string direto')
        if value.strip() == '':
            return None
        if "''" in value:
            return value.replace("''","', '")
        elif "'" in value:
            value = value.replace("'", "")
            return f'{value}'
        # print(value)
        return f'{value}'

def is_phone_number(key, value):
    if key.lower() == 'telefone' or key == 'celular' or key.lower() == 'número' or key.lower() == 'numero':
        return True
    elif '-' in value and '(' in value:
        celular_pattern = re.compile(r'^(\(?\d{2}\)?\s?)?9?\s?\d{4}-\d{4}$')
        if celular_pattern.match(value):
            return True
        else:
            return False
    else:
        celular_pattern = re.compile(r'^(?:\(?\d{2}\)?\s?)?9?\s?\d{4}-\d{4}$|^\d{11}$|^\d{10}$')
        if celular_pattern.match(value):
            return True
        else:
            return False

def is_number(str_value):
    if str_value.isdigit():
        return True
    elif str_value.replace('  ', ' ').replace('.', '').replace(',', '').replace('-','').replace(' ', '').isdigit():
        return True
    else:
        return False

def replace_number_type(key, number) -> Union[int, float, str]:

    def convert_string_to_number_string(number):
        number = number.replace('.', '')
        number = number.replace(',', '.')
        number = number.strip()
        return number

    def is_digit(number: str) -> bool:
        number = convert_string_to_number_string(number)
        
        try:
            float_value = float(number)
            return True
        except ValueError:
            return False

    def convert_number(number: str) -> Union[int, float]:        
        number = convert_string_to_number_string(number)
        if '.' in number:
            return float(number)
        else:
            return int(number)

    if is_digit(convert_string_to_number_string(number)):
        number = convert_number(convert_string_to_number_string(number))
        return number
    else:
        # print(f'chave: {key}')
        # print(f'formato errado: {number}')
        return f'{number}'
        # raise Exception('Formato de valor inválido! Tente usar um número com pontuação brasileira pra retornar em formato americano')

def push_to_database(JSON:dict):
    with open('settings.json','r') as file:
        config = json.load(file)
    db = DataBase(config["postgres"])
    errors = 0
    for report in JSON.get("reports", []):
        for report_categorie, report_categorie_value in report.items():
            print(f'Categoria: {report_categorie}')
            # print(report_categorie)
            for especific_report in report_categorie_value:
                for table_name, table_values in especific_report.items():
                    table_name = replace_to_postgres_name(table_name)
                    print(f'Tabela: {table_name}')
                    # if table_name != 'total_faturado_pelas_categorias_de_servicos':
                        # continue
                    for row in table_values:
                        print('--------')
                        if table_values[0] == row:
                            complete = False
                            for table_key, table_value in row.items():
                                if complete == False:
                                    db._create_table_if_not_exists(table_name, row)
                                    complete = True
                                errors = db.insert_in_table(table_name, row, errors)
                                # exit()
                                # sleep(1)
                                # print(f'{table_key}: {table_value}')
                        else:
                            for table_key, table_value in row.items():
                                errors = db.insert_in_table(table_name, row, errors)
                                # print(f'{table_key}: {table_value}')
    print(f'Total de erros: {errors}')

if __name__ == '__main__':
    with open('settings.json','r') as file:
        config = json.load(file)
    db = DataBase(config["postgres"])

    with open('reports.json', 'r') as file:
        print('carregando JSON...')
        JSON = json.load(file)
        if platform.system() == 'Windows':
            system('cls')
        else:
            system('clear')

    # print(db.select_table())
    # db._drop_all_tables()

    push_to_database(JSON)

    # db.insert_in_repositorys("Python", "Python is a programming language", "https://www.python.org/", [''])
    # db._delete_table('repositorys')