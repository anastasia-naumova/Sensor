import psycopg2
import time


time_start = time.time()


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


def execute_sql_resalt(connection, sql_select_query):
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    result = cursor.fetchall()
    cursor.close()
    return result


def execute_sql_query(connection, sql_querty):
    cursor = connection.cursor()
    cursor.execute(sql_querty)
    connection.commit()
    cursor.close()


def create_resalt_table(connection, name_in_meta_table, table_name_count_record, sql_delete_querty, sql_insert_querty,
                        sql_update_querty):
    cursor = connection.cursor()
    if execute_sql_resalt(connection, name_in_meta_table) == execute_sql_resalt(connection, table_name_count_record):
        execute_sql_query(connection, sql_delete_querty)
        execute_sql_query(connection, sql_insert_querty)
        execute_sql_query(connection, sql_update_querty)
    else:
        time_now = time.time()
        if time_now - time_start < 30:
            time.sleep(3)
            create_resalt_table(connection, name_in_meta_table, table_name_count_record, sql_delete_querty,
                                sql_insert_querty, sql_update_querty)
        else:
            try:
                create_resalt_table(connection, name_in_meta_table, table_name_count_record, sql_delete_querty,
                                    sql_insert_querty, sql_update_querty)
            except:
                raise TimeoutError('Время ожидания истекло')

connection = create_connection("demo", "annaum", "*****", "192.168.1.***", "5432")

name_in_meta_table = "select table_name from target_bookings.last_taken_data where jobe_name = 'a001'"

table_name_count_record = "select us.table_name from bookings.update_status us " \
                          "where us.table_name = 'flights' and insert_dttm > (select cut_value " \
                          "from target_bookings.last_taken_data where jobe_name = 'a001' and table_name = 'flights') " \
                          "union select us.table_name from bookings.update_status us " \
                          "where us.table_name = 'ticket_flights' and insert_dttm > (select cut_value " \
                          "from target_bookings.last_taken_data where jobe_name = 'a001' and " \
                          "table_name = 'ticket_flights') union select us.table_name from bookings.update_status us " \
                          " where us.table_name = 'tickets' and insert_dttm > (select cut_value " \
                          "from target_bookings.last_taken_data where jobe_name = 'a001' and table_name = 'tickets')"

sql_delete_querty = "delete from target_bookings.results_table_a001"

sql_insert_querty = "insert into target_bookings.results_table_a001 select f.flight_id, flight_no, departure_airport," \
                    "actual_departure, arrival_airport, actual_arrival, tf.ticket_no, fare_conditions, " \
                    "passenger_id from bookings.flights f join bookings.ticket_flights tf " \
                    "on f.flight_id = tf.flight_id join bookings.tickets t on tf.ticket_no = t.ticket_no " \
                    "where f.arrival_airport = 'ROV'"

sql_update_querty = "update target5еа чрнисо7 гс7рг7мпгт _bookings.last_taken_data set dataflow_dttm = dataflow_dttm + interval '1 day', " \
                    "cut_value = (select max(insert_dttm ) from bookings.update_status " \
                    "where target_bookings.last_taken_data.table_name = bookings.update_status.table_name) " \
                    "where jobe_name = 'a001'"

create_resalt_table(connection, name_in_meta_table, table_name_count_record, sql_delete_querty, sql_insert_querty,
                    sql_update_querty)
connection.close()
