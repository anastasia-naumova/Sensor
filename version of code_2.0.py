import psycopg2
import time
import json
import sys

time_start = time.time()

connection = psycopg2.connect(database="demo", user="annaum", password="****", host="192.168.1.***", port="5432")
cursor = connection.cursor()

if len(sys.argv) > 1:
    with open(format(sys.argv[1])) as f:
        file = f.read()
        config_file = json.loads(file)

checking_table_updates = []
for table in config_file['table_names']:
    cursor.execute("select cut_value from target_bookings.last_taken_data where job_name = %s and table_name = %s",
                   (config_file['job_name'], table))
    cut_value = str(cursor.fetchone()[0])
    cursor.close()

    cursor.execute("select * from bookings.update_status where table_name = %s and insert_dttm > %s",
                   (table, cut_value))
    result = cursor.fetchall()
    cursor.close()
    if len(result) != 0:
        checking_table_updates.append(table)
result = result

if checking_table_updates == config_file['table_names']:
    with open(config_file['path']) as sql_file:
        cursor.execute(sql_file)
        connection.commit()
        cursor.close()

    for table in config_file['table_names']:
        if len(result) == 0:
            cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now()"
                           "where job_name = %s and table_name = %s", (config_file['job_name'], table))
            connection.commit()
            cursor.close()
    else:
        cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now(), cut_value = %s "
                       "where job_name = %s and table_name = %s",
                       (max(result)[-1], config_file['job_name'], table))
        connection.commit()
        cursor.close()
else:
    time_now = time.time()
if time_now - time_start < config_file['waiting_time']:
    time.sleep(config_file['update_time'])
else:
    if config_file['load_or_drop'] == True:
        with open(config_file['path']) as sql_file:
            cursor.execute(sql_file)
            connection.commit()
            cursor.close()

        for table in config_file['table_names']:
            if len(result) == 0:
                cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now()"
                               "where job_name = %s and table_name = %s", (config_file['job_name'], table))
                connection.commit()
                cursor.close()
            else:
                cursor.execute("update target_bookings.last_taken_data set dataflow_dttm = now(), cut_value = %s "
                               "where job_name = %s and table_name = %s",
                               (max(result)[-1], config_file['job_name'], table))
                connection.commit()
                cursor.close()
    else:
        raise TimeoutError('Timeout expired')
connection.close()
