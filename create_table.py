import psycopg2


def create_tables():

    tables =(
        """ CREATE TABLE PointMachineData(
        station_name varchar(255) not null
        )
        """
        ,

        """ CREATE TABLE PointMachineData_1(
                station_area varchar(255) not null
                )
                """)

    conn =None
    try:
      conn = psycopg2.connect(user="postgres",
      password="Ankur@1998", host="127.0.0.1", port="5432",
      database="Point Machine Data")
      cur =conn.cursor()
      for table in tables:
        cur.execute(table)

        cur.close()
        conn.commit()
    except (Exception,psycopg2.DatabaseError) as error:
        print(error)
    finally:
       if conn is not None:
          conn.close()


if __name__ == '__main__':
    create_tables()




