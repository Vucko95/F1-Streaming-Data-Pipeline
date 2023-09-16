import psycopg2

def create_source(source_name):
    try:
        conn = psycopg2.connect(host="localhost", port=4566,user="root", dbname="dev")
        cursor = conn.cursor()

        sql_statement = f"""
        CREATE SOURCE IF NOT EXISTS {source_name} (
            race_id int,
            driver_id int,
            lap int,
            pos int,
            ts string
        )
        WITH (
            connector = 'kafka',
            topic = 'F1Topic',
            properties.bootstrap.server='kafka:9092',
            scan.startup.mode = 'earliest'
        )
        ROW FORMAT JSON;
        """

        cursor.execute(sql_statement)
        conn.commit()
        print("SQL statement executed successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")