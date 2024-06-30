import psycopg2


def create_source(source_name):
    try:
        conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev")
        cursor = conn.cursor()

        sql_statement = f"""
        CREATE SOURCE IF NOT EXISTS {source_name} (
            speed float,
            throttle float,
            brake float,
            session_time float,
        )
        WITH (
            connector = 'kafka',
            topic = 'driver_car_stream',
            properties.bootstrap.server='kafka:9092',
            scan.startup.mode = 'earliest'
        )
        ROW FORMAT JSON;
        """

        cursor.execute(sql_statement)
        conn.commit()
        print("Data Source Created successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")


def create_table(table_name):
    try:
        conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev")
        cursor = conn.cursor()

        sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            speed float,
            throttle float,
            brake float,
            session_time float
        );
        """

        cursor.execute(sql_statement)
        conn.commit()
        print(f"Table {table_name} created successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")
