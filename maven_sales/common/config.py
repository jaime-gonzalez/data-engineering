import sys
import os

script_directory = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_directory,'..', 'common'))

config_directory = os.path.normpath(os.path.join(script_directory, '..', 'config'))
schema_file_path = os.path.join(config_directory, 'schemas.json')
credentials_file_path = os.path.join(config_directory, 'postgres_credentials.json')
jdbc_path = os.path.join(config_directory, 'postgresql-42.6.0.jar')