import psycopg2
import time
import json
import sys


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port)
    except(Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL table", error)
    return connection


def execute_sql_resalt(connection, job_name, table_names):
    cursor = connection.cursor()
    cursor.execute("select cut_value from target_bookings.last_taken_data where job_name = %s and table_name = %s",
                   (job_name, table_names))
    cut_value = str(cursor.fetchone()[0])
    cursor.execute("select * from bookings.update_status where table_name = %s and insert_dttm > %s",
                   (table_names, cut_value))
    result = cursor.fetchall()
    cursor.close()
    return result


def update_sql_query(connection, job_name, table_names):
    cursor = connection.cursor()
    for table in table_names:
        if len(execute_sql_result(connection, job_name, table)) == 0:
            cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now()" 
                           "where job_name = %s and table_name = %s", (job_name, table))
        else:
            cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now(), cut_value = %s "
                           "where job_name = %s and table_name = %s",
                           (max(execute_sql_result(connection, job_name, table))[-1], job_name, table))
    connection.commit()
    cursor.close()


def create_resalt_table(connection, job_name, table_names, sql_file):
    cursor = connection.cursor()
        with open(sql_file) as file:
        for line in file:
            cursor.execute(line)
            connection.commit()
    cursor.close()
    update_sql_query(connection, job_name, table_names)
          

def sensor(connection, job_name, table_names, waiting_time, update_time, load_or_drop, sql_file):
    cursor = connection.cursor()     
    checking_table_updates = []
    for table in table_names:
        if len(execute_sql_resalt(connection, job_name, table)) !=0:
            checking_table_updates.append(table)
    if checking_table_updates == table_names:
        create_result_table(connection, job_name, table_names, sql_file)
    else:
        time_now = time.time()
        if time_now - time_start < waiting_time:
            time.sleep(update_time)
            sensor(connection, job_name, table_names, waiting_time, update_time, load_or_drop, sql_file)
        else:
            if load_or_drop == True:
                create_resalt_table(connection, job_name, table_names, sql_file)
            else:
                raise TimeoutError('Время ожидания истекло')
             
if __name__ == "__main__":
    time_start = time.time()
    
    connection = create_connection("demo", "annaum", "*****", "192.168.1.***", "5432")
    
       if len(sys.argv) > 1:
        with open(format(sys.argv[1])) as f:
            file = f.read()
            config_file = json.loads(file)

    job_name = __file__.split('/')[-1]

    sensor(connection, job_name, config_file['table_names'], config_file['waiting_time'], config_file['update_time'], 
           config_file['load_or_drop'], config_file['path'])
    
    connection.close()
