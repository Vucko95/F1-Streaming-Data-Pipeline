import psycopg2


def create_mt_live_positions():
    try:
        conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev")
        cursor = conn.cursor()

        view_name = "live_positions"
        source_name = "f1_lap_times"

        sql_statement = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        SELECT 
            r.name AS circuit, 
            CONCAT(d.forename, ' ', d.surname) AS driver,
            f1.lap,
            f1."position",  -- Enclose position in double quotes
            SUBSTRING(f1.ts, 15, 19) AS lap_time
        FROM f1_lap_times f1
        INNER JOIN drivers d ON f1.driverId = d.driverId
        INNER JOIN races r ON f1.raceId = r.raceId
        WHERE lap IN (SELECT MAX(lap) FROM f1_lap_times)
        AND f1."position" IN (1, 2, 3) 
        ORDER BY f1."position"
        LIMIT 3;
        """

        cursor.execute(sql_statement)
        conn.commit()
        print("Materialized View live_positions  created successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")
