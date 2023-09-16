import psycopg2

def create_materialized_view(view_name, source_name):
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=4566,
            user="root",
            dbname="dev"
        )
        cursor = conn.cursor()

        sql_statement = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        SELECT *
        FROM {source_name};
        """

        cursor.execute(sql_statement)
        conn.commit()
        print("SQL statement executed successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statement.")