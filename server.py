from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import json
import socket
from db_class import db_strg
db_con = db_strg()
app = Flask(__name__) 
CORS(app)
app.config['SECRET_KEY'] = "qlh-20080104"

@app.route('/ntfy_reg_device/<data>', methods = ['POST'])
def ntfy_reg_device(data):
    arr = json.loads(data)
    res = db_con.set_notification_token(arr)
    return f"Device token: {arr['token_id']}"

@app.route('/') 
def home(): 
    return "API access only"

@app.route('/<filter>') 
def filtered_booking(filter): 
    return "API access only"

@app.route('/clear_notification/<id>', methods = ['GET']) 
def clear_notification(id): 
    res = db_con.clear_notification(id)
    
    return res

@app.route('/active_bookings') 
def active_bookings(): 
    res = db_con.get_booked_services({'filter':'ORDER BY timestamp DESC'})
    return jsonify(res)

@app.route('/completed_bookings') 
def completed_bookings(): 
    res = db_con.get_completed_bookings()
    return jsonify(res)

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
    if rows != "":
        return jsonify(rows)
    else:
        return rows

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
    if res != "exist" and res != "invalid":
        return jsonify(res)
    else:
        return res

@app.route('/validate_otp/<creden>', methods = ['POST']) 
def validate_otp(creden): 
    arr = json.loads(creden)
    res = db_con.check_otp(arr)
    return res

@app.route('/request_reset_password/<uname>', methods = ['POST']) 
def request_reset_password(uname): 
    res = db_con.request_reset_password(uname)
    if res != "invalid":
        return jsonify(res)
    else:
        return res

@app.route('/reset_password/<creden>', methods = ['POST']) 
def reset_password(creden): 
    arr = json.loads(creden)
    res = db_con.reset_password(arr)
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
    return jsonify(res)

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


# Admin route

@app.route('/get_admin_notifications', methods = ['GET']) 
def get_admin_notifications(): 
    res = db_con.get_admin_notifications()
    return jsonify(res) 

@app.route('/get_shop', methods = ['GET']) 
def get_shop(): 
    res = db_con.get_shop()
    return jsonify(res) 

@app.route('/mod_tbl_shop/', methods = ['POST']) 
def mod_tbl_shop(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_shop(json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/get_threads_admin', methods = ['GET']) 
def get_threads_admin(): 
    res = db_con.get_threads_admin()
    return jsonify(res) 

@app.route('/get_thread_messages_admin/<id>', methods = ['GET']) 
def get_thread_messages_admin(id): 
    res = db_con.get_thread_messages_admin(id)
    return jsonify(res) 

@app.route('/set_thread_reply_admin/', methods = ['POST']) 
def set_thread_reply_admin(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.set_thread_reply_admin(json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/mod_tbl_threads/', methods = ['POST']) 
def mod_tbl_threads(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_threads(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/get_billing_payments/<id>', methods = ['GET']) 
def get_billing_payments(id): 
    res = db_con.get_billing_payments(id)
    return jsonify(res) 

@app.route('/get_rewards_list', methods = ['GET']) 
def get_rewards_list(): 
    res = db_con.get_rewards_list()
    return jsonify(res) 

@app.route('/get_booking_pack/<id>', methods = ['GET']) 
def get_booking_pack(id): 
    res = db_con.get_booking_pack(id)
    return jsonify(res) 

@app.route('/mod_tbl_bookings/', methods = ['POST']) 
def mod_tbl_bookings(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_bookings(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/mod_tbl_rewards/', methods = ['POST']) 
def mod_tbl_rewards(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_rewards(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/mod_tbl_services/', methods = ['POST']) 
def mod_tbl_services(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_services(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/mod_tbl_addons/', methods = ['POST']) 
def mod_tbl_addons(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_addons(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/mod_tbl_billings/', methods = ['POST']) 
def mod_tbl_billings(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_billings(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

@app.route('/mod_tbl_users/', methods = ['POST']) 
def mod_tbl_users(): 
    json_data = request.get_json()
    if json_data['tok'] == app.config['SECRET_KEY']:
        res = db_con.mod_tbl_users(json_data['act'], json_data['data'])
    else:
        res = {"res":"invalid"}
    
    return jsonify(res) 

# Admin route
  
if __name__ == '__main__': 
    host = socket.gethostbyname(socket.gethostname())
    port = os.environ.get('PORT', 10002)
    app.run(host=host, port=port, debug = True)
    