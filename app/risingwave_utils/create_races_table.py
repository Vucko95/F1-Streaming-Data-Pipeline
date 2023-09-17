import psycopg2

def create_races_table():
    try:
        conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev")
        cursor = conn.cursor()

        sql_statement = """
        CREATE TABLE IF NOT EXISTS races (
            raceId INT,
            year INT,
            round INT,
            circuit_id INT,
            name VARCHAR,
            race_date DATE,
            race_time TIME WITHOUT TIME ZONE,
            url VARCHAR,
            fp1_date DATE,
            fp1_time TIME WITHOUT TIME ZONE,
            fp2_date DATE,
            fp2_time TIME WITHOUT TIME ZONE,
            fp3_date DATE,
            fp3_time TIME WITHOUT TIME ZONE,
            quali_date DATE,
            quali_time TIME WITHOUT TIME ZONE
        );
        """

        cursor.execute(sql_statement)

        insert_data_sql = """
        INSERT INTO races VALUES 
            (1107,2023,9,70,'Austrian Grand Prix','2023-07-02','13:00:00','http://en.wikipedia.org/wiki/2022_Monaco_Grand_Prix','2023-06-30','12:00:00','2023-07-01','15:00:00','2023-06-30','11:00:00','2023-06-30','14:00:00');
        """
        cursor.execute(insert_data_sql)

        conn.commit()
        print("SQL statement executed successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")
