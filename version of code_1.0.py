import psycopg2


conn = psycopg2.connect(database="demo", user="annaum", password="****", host="192.168.1.***", port="5432")
cursor = conn.cursor()

cursor.execute("select distinct table_name from target_bookings.meta_table mt ")
results = cursor.fetchall()

root_table_name = []
for i in results:
    root_table_name.append(i[0])
print(root_table_name)

cursor.execute("select b.table_name, count(*) from bookings.meta_table b left join target_bookings.meta_table tb "
               "on b.insert_dttm = tb.cut_value where insert_dttm > '03-08-2017 07:00:00' group by b.table_name")
results2 = cursor.fetchall()
print(results2)

if len(root_table_name) == len(results2):
    cursor.execute("delete from target_bookings.results_table")
    cursor.execute("insert into target_bookings.results_table select f.flight_id, flight_no, departure_airport,"
                   " actual_departure, arrival_airport, actual_arrival, tf.ticket_no, fare_conditions, "
                   "passenger_id from bookings.flights f join bookings.ticket_flights tf on f.flight_id = tf.flight_id "
                   "join bookings.tickets t on tf.ticket_no = t.ticket_no "
                   "where f.arrival_airport = 'ROV' order by f.flight_id")
    cursor.execute("update target_bookings.meta_table set dataflow_dttm = dataflow_dttm + interval '1 day', "
                   "cut_value = (select max(insert_dttm ) from bookings.meta_table "
                   "where target_bookings.meta_table.table_name = bookings.meta_table.table_name)")
    
    conn.commit()

cursor.close()
conn.close()
