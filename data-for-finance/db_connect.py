from sqlalchemy import create_engine
import pathlib
import json


class GetData:

    @staticmethod
    def connection_string():

        credentials = None
        with open('/Users/sibelwuersch/login_information.json') as f:
            credentials = json.load(f)

            host = credentials['host']
            user = credentials['user']
            password = credentials['password']
            database = credentials['database']
            port = credentials['port']


        # Connection string and sqlalchemy engine creation
        
        
        db_connection_str = 'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(user, password, host, port, database)

        # File for SSL locally added - not pushed to GitHub -- needs to be replaced to run code
        ssl_args = {"ssl_ca": "/Users/sibelwuersch/rds-combined-ca-bundle.pem"}

        db_connection = create_engine(db_connection_str, connect_args=ssl_args)

        return db_connection