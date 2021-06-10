import pandas as pd
import sqlite3

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except:
        print("error")

    return conn

def create_table(conn):
    """ create a table 
    :param conn: Connection object
    :return:
    """
    try:
        cursor = conn.cursor()

        #Creating a dataframe. This will give us the intermediate table
        dataframe = pd.read_sql_query("SELECT * FROM Customers",conn)

        #list of unique countries
        unique_country_list = dataframe.Country.unique()

        for country in unique_country_list:
            select_query = """CREATE TABLE {country} AS
                    SELECT * FROM Customers 
                    WHERE Country = {country} """.format(country=country)
            cursor.execute(select_query)

    except:
        print("error")

def main():
    database = r"C:\sqlite\db\incubyte.db"

    # create a database connection
    conn = create_connection(database)

    if conn is not None:
        # create projects table
        create_table(conn)

    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()