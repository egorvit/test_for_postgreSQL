import psycopg2
from config import db_host, db_user, db_name, db_password
import random
import time


def create_table(data_base):
    with data_base.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE tests(
                id serial PRIMARY KEY,
                for_test varchar(180) NOT NULL);"""
        )

        print("[INFO] Table created successfully")


def insert_data(data_base):
    with data_base.cursor() as cursor:
        timing = time.time()
        rpm_insert = 0
        while True:
            if time.time() - timing < 60.0:
                values = [random.randint(1, 9) for i in range(128)]
                string_values = ''.join(str(i) for i in values)
                cursor.execute(
                    f"""INSERT INTO tests (for_test) VALUES
                    ({string_values});"""
                )
                rpm_insert += 1
                print("[INFO] Data was successfully inserted, actual rpm {rpm}".format(rpm=rpm_insert))
            else:
                break
        return rpm_insert


def get_data(data_base):
    with data_base.cursor() as cursor:
        timing = time.time()
        rpm_select = 0
        while True:
            if time.time() - timing < 60.0:
                cursor.execute(
                    """SELECT for_test FROM tests """
                )
                rpm_select += 1
                print('[INFO] Data was successfully selected, actual rpm {rpm}'.format(rpm=rpm_select))
            else:
                break
        return rpm_select


def delete_table(data_base):
    with data_base.cursor() as cursor:
        cursor.execute(
            """DROP TABLE tests;"""
        )

        print("[INFO] Table was deleted")


def test_connection(db_host, db_name, db_user, db_password, db_port=5432):
    start_time = time.time()
    i = 0
    drop_count = 0
    dict_result = dict()

    while i < 1000:
        i += 1
        try:
            conn = psycopg2.connect(host=db_host, database=db_name, user=db_user,
                                    password=db_password)
            print(i, "! START CONNECTION ! %s " % (time.time() - start_time))
            if drop_count == 1:
                dict_result['start_time'] = time.time() - dict_result['stop_time'] - start_time
                return round(dict_result['start_time'], 2)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.commit()
            print(i, "! end connection ! %s " % (time.time() - start_time))
            cur.close()
            conn.close()
            time.sleep(0.1)
        except Exception:
            time.sleep(0.1)
            if drop_count == 0:
                dict_result['stop_time'] = time.time() - start_time
                drop_count += 1
            print(i, "! CONNECTION ERROR ! %s " % (time.time() - start_time))


try:
    connection = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )
    connection.autocommit = True

    ## For rps tests

    # create_table(connection)
    # rpm_insert = insert_data(connection)
    # rpm_select = get_data(connection)
    # delete_table(connection)
    # print('RPS insert: ', round(rpm_insert / 60, 2))
    # print('RPS select: ', round(rpm_select / 60, 2))

    ## For time reconnection test

    result_test_connection = test_connection(db_host, db_name, db_user, db_password)
    print('Время восстановления подключения: ', result_test_connection)

except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
