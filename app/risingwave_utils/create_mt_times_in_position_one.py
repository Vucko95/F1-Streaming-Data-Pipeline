import psycopg2

def create_mt_times_in_position_one():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=4566,
            user="root",
            dbname="dev"
        )
        cursor = conn.cursor()

        view_name = "times_in_position_one"

        sql_statement = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        SELECT 
            CONCAT(d.forename, ' ', d.surname) AS driver,
            SUM(f1."position") AS num_of_position_1_laps
        FROM f1_lap_times f1
        INNER JOIN drivers d ON f1.driverId = d.driverId
        INNER JOIN races r ON f1.raceId = r.raceId
        WHERE f1."position" = 1
        GROUP BY CONCAT(d.forename, ' ', d.surname)
        ORDER BY SUM(f1."position") DESC;
        """

        cursor.execute(sql_statement)
        conn.commit()
        print("SQL statement executed successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")
