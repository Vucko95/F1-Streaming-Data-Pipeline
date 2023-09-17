import psycopg2

def create_drivers_table_and_add_data():
    try:
        conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev")
        cursor = conn.cursor()

        # Create the "drivers" table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS drivers (
            driverId INT,
            driver_ref VARCHAR,
            number INT,
            code VARCHAR,
            forename VARCHAR,
            surname VARCHAR,
            dob DATE,
            nationality VARCHAR,
            url VARCHAR
        );
        """
        cursor.execute(create_table_sql)

        insert_data_sql = """
        INSERT INTO drivers VALUES
            (844,'leclerc',16,'LEC','Charles','Leclerc','1997-10-16','Monegasque','http://en.wikipedia.org/wiki/Charles_Leclerc'),
            (832,'sainz',55,'SAI','Carlos','Sainz','1994-09-01','Spanish','http://en.wikipedia.org/wiki/Carlos_Sainz_Jr.'),
            (815,'perez',11,'PER','Sergio','Pérez','1990-01-26','Mexican','http://en.wikipedia.org/wiki/Sergio_P%C3%A9rez'),
            (830,'max_verstappen',33,'VER','Max','Verstappen','1997-09-30','Dutch','http://en.wikipedia.org/wiki/Max_Verstappen'),
            (846,'norris',4,'NOR','Lando','Norris','1999-11-13','British','http://en.wikipedia.org/wiki/Lando_Norris'),
            (847,'russell',63,'RUS','George','Russell','1998-02-15','British','http://en.wikipedia.org/wiki/George_Russell_%28racing_driver%29'),
            (4,'alonso',14,'ALO','Fernando','Alonso','1981-07-29','Spanish','http://en.wikipedia.org/wiki/Fernando_Alonso'),
            (1,'hamilton',44,'HAM','Lewis','Hamilton','1985-01-07','British','http://en.wikipedia.org/wiki/Lewis_Hamilton'),
            (20,'vettel',5,'VET','Sebastian','Vettel','1987-07-03','German','http://en.wikipedia.org/wiki/Sebastian_Vettel'),
            (839,'ocon',31,'OCO','Esteban','Ocon','1996-09-17','French','http://en.wikipedia.org/wiki/Esteban_Ocon'),
            (852,'tsunoda',22,'TSU','Yuki','Tsunoda','2000-05-11','Japanese','http://en.wikipedia.org/wiki/Yuki_Tsunoda'),
            (822,'bottas',77,'BOT','Valtteri','Bottas','1989-08-28','Finnish','http://en.wikipedia.org/wiki/Valtteri_Bottas'),
            (825,'kevin_magnussen',20,'MAG','Kevin','Magnussen','1992-10-05','Danish','http://en.wikipedia.org/wiki/Kevin_Magnussen'),
            (817,'ricciardo',3,'RIC','Daniel','Ricciardo','1989-07-01','Australian','http://en.wikipedia.org/wiki/Daniel_Ricciardo'),
            (854,'mick_schumacher',47,'MSC','Mick','Schumacher','1999-03-22','German','http://en.wikipedia.org/wiki/Mick_Schumacher'),
            (848,'albon',23,'ALB','Alexander','Albon','1996-03-23','Thai','http://en.wikipedia.org/wiki/Alexander_Albon'),
            (842,'gasly',10,'GAS','Pierre','Gasly','1996-02-07','French','http://en.wikipedia.org/wiki/Pierre_Gasly'),
            (855,'zhou',24,'ZHO','Guanyu','Zhou','1999-05-30','Chinese','http://en.wikipedia.org/wiki/Guanyu_Zhou'),
            (840,'stroll',18,'STR','Lance','Stroll','1998-10-29','Canadian','http://en.wikipedia.org/wiki/Lance_Stroll'),
            (849,'latifi',6,'LAT','Nicholas','Latifi','1995-06-29','Canadian','http://en.wikipedia.org/wiki/Nicholas_Latifi');
        """
        cursor.execute(insert_data_sql)

        conn.commit()
        print("SQL statements executed successfully.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)
        print("Failed to execute SQL statements.")
