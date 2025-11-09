"""
The everything 
"""
from data_source import data_generator
from database.database import (create_database,post_database,read_database)

def main():
    count = 10
    counter = 0
    create_database()
    while True:
        if counter == count:
            read_database()
        data = data_generator()
        post_database(data)
        counter+=1



if __name__ == "__main__":
    main()

