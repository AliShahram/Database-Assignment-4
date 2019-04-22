import psycopg2
import csv

class DatabaseConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(user = "",
                            password = "",
                            host = "127.0.0.1",
                            port = "5432",
                            database = "postgres")
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Connection Successful")
        except (Exception) as error:
            print("Could not connect to the database:", error)


    def close_connection(self):
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


    def create_table(self, name):
        try:
            create_table_command = "CREATE TABLE " + name + """(
                    Lon  decimal,
                    Lat   decimal,
                    Number    varchar(20),
                    Street  varchar(100),
                    Unit  varchar(100),
                    City  varchar(100),
                    District  varchar(100),
                    Region varchar(100),
                    PostCode  varchar(20),
                    Id    varchar(20),
                    hash  varchar(150) PRIMARY KEY    NOT NULL
            )"""
            self.cursor.execute(create_table_command)
            print("Table " + name + " was successfuly created")

        except (Exception, psycopg2.DatabaseError) as error:
            print ("Error while creating PostgreSQL table", error)


    def delete_table(self, name):
        try:
            delete_table_command = "DROP TABLE " + name
            self.cursor.execute(delete_table_command)
            print("Table " + name + " successfuly dropped")
        except (Exception, psycopg2.DatabaseError) as error:
            print ("Error while dropping PostgreSQL table", error)


    def insert_record(self, record, table):
        try:
            insert_command = "INSERT INTO " + table + """(
                    Lon,
                    Lat,
                    Number,
                    Street,
                    Unit,
                    City,
                    District,
                    Region,
                    PostCode,
                    Id,
                    hash) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            #record_to_insert = (record[0], record[1], record[2], record[3],
                                #record[4], record[5], record[6], record[7],
                                #record[8], record[9], record[10])

            self.cursor.execute(insert_command, record)
            print("Record successfuly added to table " + table)

        except (Exception, psycopg2.DatabaseError) as error:
            print ("Error while adding record to PostgreSQL table", error)



class ParseFile:
    def __init__(self):
        pass

    def parse_row(self, row):
        line = ",".join(row)
        record = line.split(",")
        record_to_insert = (record[0], record[1], record[2], record[3],
                            record[4], record[5], record[6], record[7],
                            record[8], record[9], record[10])
        return record_to_insert





if __name__ == '__main__':
    database = DatabaseConnection()
    file = ParseFile()
    #database.create_table("austria")

    with open('austria.csv') as csv_file:
        csv_reader = csv.reader(csv_file)
        line_count = 0

        for row in csv_reader:
            record = file.parse_row(row)
            database.insert_record(record, 'austria')
