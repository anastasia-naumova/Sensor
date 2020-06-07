import psycopg2
import time
import json
import sys


def cast_argument_to_dict(command_line_arguments):
    if len(command_line_arguments) > 1:
        with open(format(command_line_arguments[1])) as f:
            file = f.read()
            config_file = json.loads(file)
    return config_file


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL table", error)
    return connection


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


def get_cut_value_for_tables(connection, table_names, job_name):
    cursor = connection.cursor()
    cut_value = {}
    for table in table_names:
        cursor.execute("select cut_value from target_bookings.last_taken_data where job_name = %s and table_name = %s",
                       (job_name, table))
        cut_value[table] = str(cursor.fetchone()[0])
    cursor.close()
    return cut_value


def check_table_status(connection, table_names, job_name, waiting_time, update_time):
    cursor = connection.cursor()
    table_statuses = {}
    checking_table_updates = []
    cut_value = get_cut_value_for_tables(connection, table_names, job_name)
    while time.time() - time_start < waiting_time and compare_lists(table_names, table_statuses.keys()):
        checking_table_updates.clear()
        for table in cut_value:
            cursor.execute(
                "select max(insert_dttm) from bookings.update_status where table_name = %s and insert_dttm > %s",
                (table, cut_value[table]))
            result = str(cursor.fetchone()[0])
            if result != 'None':
                table_statuses[table] = result
        if not compare_lists(table_names, table_statuses.keys()):
            time.sleep(update_time)
    cursor.close()
    return table_statuses


def create_result_table(connection, path_to_sql_file):
    cursor = connection.cursor()
    with open(path_to_sql_file) as sql_file:
        cursor.execute(sql_file.read())
        connection.commit()
    cursor.close()

def update_cutparam(connection, table_statuses, job_name):
    cursor = connection.cursor()
    for table in table_statuses:
        cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now(), cut_value = %s "
                       "where job_name = %s and table_name = %s", (table_statuses[table], job_name, table))
    connection.commit()
    cursor.close()


def load_table(connection, table_statuses, table_names, path_to_sql_file, job_name, load_or_drop):
    if compare_lists(table_names, table_statuses.keys()) or load_or_drop == True:
        create_result_table(connection, path_to_sql_file)
        update_cutparam(connection, table_statuses, job_name)
    else:
        raise TimeoutError('Timeout expired')

if __name__ == "__main__":
    time_start = time.time()

    connection = create_connection("demo", "annaum", "123", "192.168.1.67", "5432")

    config_file = cast_argument_to_dict(sys.argv)

    table_statuses = check_table_status(connection=connection, table_names=config_file['table_names'],
                                        job_name=config_file['job_name'], waiting_time=config_file['waiting_time'],
                                        update_time=config_file['update_time'])

    load_table(connection, table_statuses=table_statuses, table_names=config_file['table_names'],
               path_to_sql_file=config_file['path'], job_name=config_file['job_name'],
               load_or_drop=config_file['load_or_drop'])

    connection.close()
