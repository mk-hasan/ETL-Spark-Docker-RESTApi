#Import all the necessary libraries.
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
"""
Author: Kamrul Hasan
Email: hasana.alive@gmail.com
Date: 20.12.2020
"""

from init import create_app



app,config= create_app()
db = SQLAlchemy(app)
orders = db.Table(config['db_table'],db.metadata,autoload=True,autoload_with=db.engine) # automap the existing table from the database

@app.route('/spend/<string:customerid>',methods=['GET']) # set the endpoint
def get_orders(customerid):
    '''
    The functions find the customer details by customerid for a GET request

    Parameters
    ----------
    customerid : str
        The argument is used for finding the corresponding order details.

	Returns
    -------
    customerDetails: json
        customerDetails contains the id, orders and totalNetMerchandiseValueEuro.
    '''
    getResults = db.session.query(orders).all()
    for result in getResults:
        if result.customerId==customerid:
            customerDetails = result 
    return jsonify({'customerId': customerDetails.customerId,'orders': customerDetails.orders,'totalNetMerchandiseValueEur':customerDetails.TotalMerchandiseValueEur })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5005)