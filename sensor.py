import psycopg2
import time
import json
import sys
from accessify import protected


def compare_lists(list1, list2):
    i = 0
    if len(list1) == len(list2):
        while i < len(list1):
            if list1[i] in list2:
                i += 1
            else:
                return False
        return True
    return False


def create_result_table(connection, path_to_sql_file):
    cursor = connection.cursor()
    try:
        with open(path_to_sql_file) as sql_file:
            cursor.execute(sql_file.read())
            connection.commit()
    except IOError:
        print('File not found or closed')
    cursor.close()


def update_cutparam(connection, table_statuses, job_name):
    cursor = connection.cursor()
    for table in table_statuses:
        cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now(), cut_value = %s "
                       "where job_name = %s and table_name = %s", (table_statuses[table], job_name, table))
    connection.commit()
    cursor.close()

#Что я хочу сделать? Что будет? Для чего мне это?
#обдумать ошибки, если нужно переделать
# 1- проверка структуры json, что есть все параметры - raise

class Config:

    def __init__(self, path_to_config_file): #передавать строку
        if len(path_to_config_file) > 1:
            try:
                with open(format(path_to_config_file[1])) as f:#переделать
                    file = f.read()
                    self.config_file = json.loads(file)
            except json.JSONDecodeError:
                print('Invalid configuration file format')
            except IOError:
                print('File not found or closed')


class Sensor(Config):#наследование здесь не нужно и нельзя делать

    def __init__(self, path_to_config_file, connection):
        super().__init__(path_to_config_file)
        self.connection = connection

    @protected
    def get_cut_value_for_tables(self):
        cursor = self.connection.cursor()
        cut_value = {}
        for table in self.config_file['table_names']:
            cursor.execute("select cut_value from target_bookings.last_taken_data where job_name = %s and "
                           "table_name = %s", (self.config_file['job_name'], table))
            cut_value[table] = str(cursor.fetchone()[0])
        cursor.close()
        return cut_value

    @protected
    def check_table_status(self):
        cursor = self.connection.cursor()
        checking_table_updates = []
        table_statuses = {}
        cut_value = self.get_cut_value_for_tables()
        while time.time() - time_start < self.config_file['waiting_time'] and not\
                compare_lists(self.config_file['table_names'], table_statuses):
            checking_table_updates.clear()
            for table in cut_value:
                cursor.execute("select max(insert_dttm) from bookings.update_status where table_name = %s "
                               "and insert_dttm > %s", (table, cut_value[table]))
                result = str(cursor.fetchone()[0])
                if result != 'None':
                    table_statuses[table] = result
            if not compare_lists(self.config_file['table_names'], table_statuses):
                time.sleep(self.config_file['update_time'])
        cursor.close()
        return table_statuses

    @protected
    def load_table(self):
        table_statuses = self.check_table_status()
        if compare_lists(self.config_file['table_names'], table_statuses) \
                or self.config_file['load_or_cruch'] == True:
            create_result_table(self.connection, self.config_file['path_to_sql_file'])
            update_cutparam(self.connection, table_statuses, self.config_file['job_name'])
        else:
            raise TimeoutError('Timeout expired')

    def waiting(self):
        self.load_table()


if __name__ == "__main__":

    time_start = time.time()

    try:
        connection = psycopg2.connect(database="demo", user="annaum", password="123", host="192.168.1.67", port="5432")
    except:
        raise psycopg2.OperationalError('Connection failure')

    load_job = Sensor(sys.argv, connection)

    load_job.waiting()

    load_job.connection.close()