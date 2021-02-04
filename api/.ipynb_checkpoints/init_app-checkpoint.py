from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import json

def create_app():
	'''
	This is a function to initialize the flask api app that also establish the database connection.
	'''
	app = Flask(__name__)

	path="./config_restapi.json"
	config=load_config(path)
	username= config['db_username']
	password= config['db_password']
	hostname = config['db_hostname']
	port = config['db_port']
	database = config['db_name']

	#app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres1234@localhost:5432/demo" # configure the database connection
	app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{username}:{password}@{hostname}:{port}/{database}' 
	db = SQLAlchemy(app)
	db.init_app(app)
	db.create_all()
	return app,config

def load_config(path):
    '''
    This function loads the database configuration file to establish database connection.

    Parameters
    ----------
    path : str
        This argument is a for reading the json configuration file

    Returns
    -------
    config: json
        The return is configuration details in a json format containing the details like database name, table, usrname,password

    '''
    
    with open(path, 'r') as j:
        config = json.loads(j.read())

    return config