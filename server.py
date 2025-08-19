#import sqlite3
from flask import Flask, jsonify, request, render_template
import json
import socket
from global_variable import g_var
from db_class import db_strg
db_con = db_strg()
app = Flask(__name__) 

@app.route('/') 
def home(): 
    res = db_con.get_booked_services({'filter':''})
    return render_template("system.html", data=res)

@app.route('/<filter>') 
def filtered_booking(filter): 
    res = db_con.get_booked_services({'filter':f'WHERE B.status=\'{filter}\''})
    return render_template("system.html", data=res)
    
@app.route('/users', methods = ['GET']) 
def users(): 
    #con = sqlite3.connect("./db/mktg_data.db")
    #data = con.custom_query("get", "SELECT * FROM tbl_products WHERE status='Active' ORDER BY product_name ASC", "*")
    rows = db_con.get_user("all")
   
    return jsonify(rows)

@app.route('/services', methods = ['GET']) 
def services(): 
    rows = db_con.get_services("all")
    return jsonify(rows)

@app.route('/addons', methods = ['GET']) 
def addons(): 
    rows = db_con.get_addons("all")
    return jsonify(rows)

@app.route('/signin/<creden>', methods = ['POST']) 
def signin(creden): 
    arr = json.loads(creden)
    res = db_con.validate_user(arr)
    if res != "invalid":
        return jsonify(res)
    else:
        return res
    
@app.route('/signup/<creden>', methods = ['POST']) 
def signup(creden): 
    arr = json.loads(creden)
    res = db_con.create_user(arr)
    return jsonify(res)

@app.route('/validate_otp/<creden>', methods = ['POST']) 
def validate_otp(creden): 
    arr = json.loads(creden)
    res = db_con.check_otp(arr)
    return res

@app.route('/get_otp/<creden>', methods = ['POST']) 
def get_otp(creden): 
    arr = json.loads(creden)
    res = db_con.retrieve_otp(arr)
    return res
    
@app.route('/set_booking/<booking_data>', methods = ['POST']) 
def set_booking(booking_data): 
    arr = json.loads(booking_data)
    
    res = db_con.create_booking(arr)
    return jsonify(res) 

@app.route('/get_booking/<booking_data>', methods = ['GET']) 
def get_booking(booking_data): 
    arr = json.loads(booking_data)
    
    res = db_con.get_booked_services(arr)
    return jsonify(res) 

@app.route('/get_threads/<threads_data>', methods = ['GET']) 
def get_threads(threads_data): 
    arr = json.loads(threads_data)
    
    res = db_con.get_booked_threads(arr)
    return jsonify(res) 

@app.route('/set_threads/<threads_data>', methods = ['POST']) 
def set_threads(threads_data): 
    arr = json.loads(threads_data)
    
    res = db_con.set_booked_threads(arr)
    return jsonify(res) 

@app.route('/map/') 
def map(): 
    return render_template("map.html") 

#Admin route
@app.route('/update_booking/', methods = ['POST']) 
def update_booking(): 
    arr = json.loads(request.data)
    
    res = db_con.update_client_booked(arr)
    return jsonify(res) 
  
if __name__ == '__main__': 
    host = socket.gethostbyname(socket.gethostname())
    app.run(host=host, port=5000, debug = True)