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
    res = db_con.get_booked_services({'filter':'ORDER BY timestamp'})
    return render_template("system.html", data=res)

@app.route('/<filter>') 
def filtered_booking(filter): 
    res = db_con.get_booked_services({'filter':f'WHERE status=\'{filter}\''})
    return render_template("system.html", data=res)

# @app.route('/drop_table/<tbl_name>') 
# def drop_table(tbl_name): 
#     res = db_con.drop_table(tbl_name)
#     return jsonify(res)

@app.route('/server-datetime') 
def server_datetime(): 
    res = db_con.get_datetime()
    dt = res.split(" ")
    return jsonify(dt)

@app.route('/users/<id>', methods = ['GET']) 
def users(id): 
    #con = sqlite3.connect("./db/mktg_data.db")
    #data = con.custom_query("get", "SELECT * FROM tbl_products WHERE status='Active' ORDER BY product_name ASC", "*")
    rows = db_con.get_user(id)
   
    return jsonify(rows)

@app.route('/services/<id>', methods = ['GET']) 
def services(id): 
    rows = db_con.get_services(id)
    return jsonify(rows)

@app.route('/addons/<id>', methods = ['GET']) 
def addons(id): 
    rows = db_con.get_addons(id)
    return jsonify(rows)

@app.route('/user_points/<id>', methods = ['GET']) 
def user_points(id): 
    rows = db_con.get_user_points(id)
    return jsonify(rows)

@app.route('/earned_points/<id>', methods = ['GET']) 
def earned_points(id): 
    rows = db_con.get_earned_points(id)
    return jsonify(rows)

@app.route('/points_history/<id>', methods = ['GET']) 
def points_history(id): 
    rows = db_con.get_points_history(id)
    return jsonify(rows)

@app.route('/usaged_points/<id>', methods = ['GET']) 
def usaged_points(id): 
    rows = db_con.get_usaged_points(id)
    return jsonify(rows)

@app.route('/points_reward/<status>', methods = ['GET']) 
def points_reward(status): 
    rows = db_con.get_points_reward(status)
    return jsonify(rows)

@app.route('/update_field/<data>', methods = ['POST']) 
def update_field(data): 
    arr = json.loads(data)
    res = db_con.set_update_field(arr)
    if res != "invalid":
        return jsonify(res)
    else:
        return res
    
@app.route('/custom_update/<sql>', methods = ['POST']) 
def custom_update(sql): 
    arr = json.loads(sql)
    res = db_con.set_custom_update(arr)
    
    return res

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

# @app.route('/get_booking/<booking_data>', methods = ['GET']) 
# def get_booking(booking_data): 
#     arr = json.loads(booking_data)
    
#     res = db_con.get_booked_services(arr)
#     return jsonify(res) 

# new mod

@app.route('/get_user_booking/<cnd>', methods = ['GET']) 
def get_user_booking(cnd): 
    arr = json.loads(cnd)
    
    res = db_con.get_booking(arr)
    return json.dumps(res)

@app.route('/get_booking_services/<cnd>', methods = ['GET']) 
def get_booking_services(cnd): 
    arr = json.loads(cnd)
    
    res = db_con.get_booking_services(arr)
    return jsonify(res) 

@app.route('/get_booking_tracks/<cnd>', methods = ['GET']) 
def get_booking_tracks(cnd): 
    arr = json.loads(cnd)
    
    res = db_con.get_booking_tracks(arr)
    return jsonify(res) 

@app.route('/get_booking_payments/<cnd>', methods = ['GET']) 
def get_booking_payments(cnd): 
    arr = json.loads(cnd)
    
    res = db_con.get_booking_payments(arr)
    return jsonify(res) 

# new mod

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