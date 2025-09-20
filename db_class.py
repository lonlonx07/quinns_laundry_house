import psycopg2
from psycopg2.extras import RealDictCursor # Or DictCursor
import random
from datetime import datetime, timedelta
import os
import json
import base64
import firebase_admin
from firebase_admin import credentials, messaging
#cred = credentials.Certificate("quinns-laundry-house-5d121-cad4a86b8589.json")
#firebase_admin.initialize_app(cred)

os.environ['SERVICE_ACCOUNT_JSON'] = 'eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogInF1aW5ucy1sYXVuZHJ5LWhvdXNlLTVkMTIxIiwgInByaXZhdGVfa2V5X2lkIjogImNhZDRhODZiODU4OWI3MDc2ZTA0ZmE4N2EzOTllOWU5OGEwYjc5YzYiLCAicHJpdmF0ZV9rZXkiOiAiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXG5NSUlFdmdJQkFEQU5CZ2txaGtpRzl3MEJBUUVGQUFTQ0JLZ3dnZ1NrQWdFQUFvSUJBUUM3aVNIK3haemZpRGY0XG51bkMrRytBRVUzcENMSjMvSjVYZ0NTcm1xeDBDTUZsMUdJQXpuUTdTd3pUc3RRZ3pHbkMwZFVqSFRuZmZtWDJXXG5iZzNYNXBEVXVmRTdmM2JZRzhWNzdBV09ZTFVUYmNGczBxM2cydXhFbklSRVljb2NQSGlIclhsSzhuT05OODgzXG5Sa1N5UG1NWVBIdmdhcjk3eFFXMWVVcXFKOEdUTXMyZzZ5RExkYys0dlVmaWtxRndBcFFaOFNYVkd0MHM0eVNDXG5lUWF6dmorSXh4YjlTNnQ5S0hXMG51YzNYTVg4c3BrVTYvdUttRHFKdk5SZHU3SlpwNTNMN0hFSmp1elBwYjhtXG4zUi9hM25Kbi82dzJ2MW8vbGp2ZG9rRTQ0ZVc5ZXgyYUl5amhPWjVrRGNrWWNQWlRSTHUwWXlSdWxKWFRXejZXXG5VNjJsNWxGakFnTUJBQUVDZ2dFQUNLdWdiTDBLTWJPRWt2VmJ2UTVnM3hKVTlyWkZYOFNldzBycE50ejJiR0Y3XG40VlFPcVZRN21UYjVQWEJwUHFPY1RsdVZ6OUVxN2FXVkR0MXJ1bTJvaUkxSVArMzJ1cXNlQ0VxelB3L3RqMFpsXG55UGc4bmNiQlliS3kwczZLbjUrVUd3SEVOOWpaeXMrbnZ2QjZ3SnJ0emZNTjd5UkFVaUtienZZYW81OU83ZFU1XG5JdkQrRlFMWkNQUllUVHBCZVpYbG02OE9SbThUdTRrNTdWNGROSkh4bVJIUkc1Snh4ZFN4elNIV3hzNDVDVHBuXG55Wnk1SVMrT2xsZzNNZTNrZDg0V2UvNkZBTWZTRXlWeEFaSGZXU0VTTVJZSlgvbmNMWUZXWVg4MVltS1c3NmFtXG5YU2R3V3RWa1p2NUpKd1p4N0w4RWRjbUo5SXZ2bUFlWUZsbmdJb3EzY1FLQmdRRHVvWThvRWNhQ0pGbTcrVHRSXG5TelFqdFBYSXpYU2pwL1NPSVd4Z1VNalVxT2wrTkVpT2l2MTg5bHM2eG04Y2JleWZVbGNZT1k2UWtUUUFZSmJzXG43bVJhMjVRY3Fad3dNZ2NpaGRWYzFHNi90TTYzbXpUY3ZMTFp5aEMvZ0VEOFFBT3RrYlNYcEFVVi90UjdEYkFlXG43TU9RUkUvTHlwV3pqQWJSNkx2OTBnc09yd0tCZ1FESkw0RzhabmcyY2s3VFFYLzZKZCtTNXJNMnR4b2c5eitTXG5uSWFhVk5Za2ZwV1pBWDBBTWxidnpWVnNnMjV2cFNBZm5BTC9peHVZUEpCNGoyTEFpMUpHcGorVEg5WWJmOEJrXG44ejl5VnFvNHIzUlduUFVhQ1pxUVRqenhua1UzQjdtQm40T1AzVDlwY0tmNDc1VC9BbVFBOU9RNU5DUmlpUFlnXG5JdDJhS2NzMWpRS0JnUURhbktsd2RQQmRzMzE5cG5NQUk1d3RoZytSR1IvYytmWWg2MTdFMGQxYWJUQVRUVVNYXG5TWisyUmw4SGRsaStPN29GcjBKWTBBbmFTUnZScUtzN2ZaMzBXcXJTbzFPU0Y1TFB2cm5icEVXZFhML3dGc0VUXG40ODhabEhOSmJPNmI2TzAxcW9FK1ZxWE9JN2wyemZCbU5GTm9yQnhLUDFwcnRVRmVOZzliRlh6Snd3S0JnQXUyXG5GMEJIV3NJWW4xd08yUXRQdnhjSEZQR2ZjUWJ3UGFRa05uV3ZjSlBKUnA0VWh4bEcxT2E2dGpsTjRWVGdjT0ZHXG5MS3FCaDRheUo5ck14ZnZkWUZtNmZjTHJ2SVAzU05UWGtCN3d5cEhvTE1hSjluNmdobjNXQUJnMGxXVGhyenZMXG5kRllnay90b1VtN2NTM2tZdzRlR3VlNVdpYk91czEwbGltN1o5ZmxWQW9HQkFKS0E2bnptQlREQW5pZGZveWZBXG4vT05UWHUweDdFU3paS0N0aUJIV25LMElSdGRBUDZ0Nm40R2tpWTRQcDl1RkprdSt2NmVFbk1Mc3dwN21mVDZpXG42T3UxbXpNR3YwNGNNQlZGdHhyMkN6aVVZZVMvS1NnWE94WTMwc0ZYT0lDRURtS1d1enJha0hLYzg1UytmejJTXG5Sc3g4WTFEUWxoR1oxZkJZQ2VLMjFYVENcbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cbiIsICJjbGllbnRfZW1haWwiOiAiZmlyZWJhc2UtYWRtaW5zZGstZmJzdmNAcXVpbm5zLWxhdW5kcnktaG91c2UtNWQxMjEuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCAiY2xpZW50X2lkIjogIjExNzM3MDQ0MjkwMDM1NjIxODA1NCIsICJhdXRoX3VyaSI6ICJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20vby9vYXV0aDIvYXV0aCIsICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLCAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsICJjbGllbnRfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9yb2JvdC92MS9tZXRhZGF0YS94NTA5L2ZpcmViYXNlLWFkbWluc2RrLWZic3ZjJTQwcXVpbm5zLWxhdW5kcnktaG91c2UtNWQxMjEuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCAidW5pdmVyc2VfZG9tYWluIjogImdvb2dsZWFwaXMuY29tIn0='
encoded_key = os.getenv("SERVICE_ACCOUNT_JSON")
if not encoded_key:
    raise ValueError("The SERVICE_ACCOUNT_JSON environment variable is not set.")

service_account_info = json.loads(base64.b64decode(encoded_key).decode('utf-8'))
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred)

import pytz
UTC = pytz.utc
ph_tz = pytz.timezone('Asia/Manila')

class db_strg:
    otp_list = {}
    ntfy_list = {}
    client_ntfy_tok = {}
    client_ntfy_dat = {}
    new_msg = []
    new_booking = []

    def __init__(self) -> None:
        creden_arr = {}
        try:
            with open("credentials.txt", "r") as f:
                data = f.readlines()
            f.close()
            for item in data:
                arr = item.split("=")
                creden_arr[arr[0]] = (arr[1]).strip()      
        except:
            pass

        services_data = {}
        try:
            with open("services.txt", "r") as f:
                services_data = f.readlines()
            f.close()
        except:
            pass
        
        addons_data = {}
        try:
            with open("addons.txt", "r") as f:
                addons_data = f.readlines()
            f.close()
        except:
            pass
        
        try:
            self.conn = psycopg2.connect(database=creden_arr['database'], user=creden_arr['user'], password=creden_arr['password'], host=creden_arr['host'], port=creden_arr['port'])
            self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
            #self.cur = self.conn.cursor()
            #conn.close()
            try:
                self.cur.execute('''CREATE TABLE tbl_users (id SERIAL PRIMARY KEY, user_name TEXT NOT NULL, password TEXT NOT NULL, email TEXT, first_name TEXT NOT NULL, last_name TEXT NOT NULL, address TEXT, mobile_no TEXT, timestamp TEXT, status TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_booking (id SERIAL PRIMARY KEY, user_id INT, schedule TEXT, mode TEXT, client TEXT, contact TEXT, pickup_loc TEXT, quantity TEXT, unit TEXT, timestamp TEXT, status TEXT, gps_coordinate TEXT, logistics_fee NUMERIC);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_booking_addon (booking_id INT, service_id INT, prod_type TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_services (id SERIAL PRIMARY KEY, category TEXT, title TEXT, sub_title TEXT, description TEXT, price numeric, quantity numeric, unit TEXT, status TEXT);''')
                for item in services_data:
                    arr = item.split("::")
                    sql = f"INSERT INTO tbl_services (category,title,sub_title,description,price,quantity,unit,status) VALUES (\'{arr[1]}\', \'{arr[2]}\', \'{arr[3]}\', \'{arr[4]}\', \'{arr[5]}\', \'{arr[6]}\', \'{arr[7]}\', \'{arr[8]}\')"
                    self.cur.execute(sql)
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_addons (id SERIAL PRIMARY KEY, category TEXT, title TEXT, description TEXT, price numeric, status TEXT);''')
                for item in addons_data:
                    arr = item.split("::")
                    sql = f"INSERT INTO tbl_addons (category,title,description,price,status) VALUES (\'{arr[1]}\', \'{arr[2]}\', \'{arr[3]}\', \'{arr[4]}\', \'{arr[5]}\')"
                    self.cur.execute(sql)
                self.conn.commit()
            except:
                self.conn.rollback() 

            try:
                self.cur.execute('''CREATE TABLE tbl_threads (id SERIAL PRIMARY KEY, booking_id INT, timestamp TEXT, status TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_thread_messages (thread_id INT, sender TEXT, message TEXT, timestamp TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_payments (id SERIAL PRIMARY KEY, booking_id INT, mode TEXT, ref_num TEXT, amount NUMERIC, status TEXT, timestamp TEXT, add_charges NUMERIC, description TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_rewards (id SERIAL PRIMARY KEY, title TEXT, description TEXT, pts_req INT, status TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_qouta (date TEXT PRIMARY KEY, day TEXT, min_qouta INT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_pts_earned (user_id INT, amount INT, source TEXT, timestamp TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_pts_used (user_id INT, amount INT, benefit TEXT, timestamp TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback() 

            try:
                self.cur.execute('''CREATE TABLE tbl_book_tracking (booking_id INT, sched TEXT, accepted TEXT, pickup TEXT, drop_off TEXT, arrived TEXT, processing TEXT, outgoing TEXT, completed TEXT, cancelled TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_shop (shop_name TEXT, address TEXT, contact TEXT, fb_page TEXT, logistics NUMERIC, free_threshold NUMERIC, 
                                 sunday TEXT, sun_stat TEXT,
                                 monday TEXT, mon_stat TEXT,
                                 tuesday TEXT, tue_stat TEXT,
                                 wednesday TEXT, wed_stat TEXT,
                                 thursday TEXT, thu_stat TEXT,
                                 friday TEXT, fri_stat TEXT,
                                 saturday TEXT, sat_stat TEXT
                                 );''')
                
                sql = f"""
                INSERT INTO tbl_shop (shop_name,address,contact,fb_page,logistics,free_threshold,sunday,sun_stat,monday,mon_stat,tuesday,tue_stat,wednesday,wed_stat,thursday,thu_stat,friday,fri_stat,saturday,sat_stat) VALUES (
                'Quinns Laundry House',
                'Zone 6, Bagong Sirang San Felipe, Naga City / beside Blue Spring Water Refilling Station',
                '+639761082555',
                'https://www.facebook.com/quinns.laundryhouse',
                35.0,
                3,
                '8:00 am-9:00 pm',
                'open',
                '8:00 am-9:00 pm',
                'open',
                '8:00 am-9:00 pm',
                'open',
                '8:00 am-9:00 pm',
                'open',
                '8:00 am-9:00 pm',
                'open',
                '8:00 am-9:00 pm',
                'open',
                '8:00 am-9:00 pm',
                'open'
                )
                """
                self.cur.execute(sql)
                self.conn.commit()

            except:
                self.conn.rollback()

            # try:
            #     self.cur.execute('''ALTER TABLE tbl_book_tracking ADD COLUMN cancelled TEXT''')
            #     self.conn.commit()
            # except:
            #     self.conn.rollback()

        except:
            print("Database connection error!")  

    def format_std_code(self, code, num, max_len):
        res = ""
        ctr = len(num)
        while ctr < max_len:
            res += "0"
            ctr += 1

        return code+res+num

    def gen_rand_num_codes(self, len):
        i=0
        code = ""
        while i < len:
            code += str(random.randint(0, 9))
            i += 1
            
        return code   

    def get_datetime(self):
        dt_now = datetime.now(ph_tz)
        dt_now = dt_now.strftime("%Y-%m-%d %H:%M:%S")

        return dt_now 
    
    def get_admin_notifications(self):  
        res1 = self.new_booking
        res2 = self.new_msg
        self.new_msg = []
        self.new_booking = []

        return {'bok':res1, 'msg':res2}

    def clear_notification(self, uid):
        res = "success"
        try:
            del self.client_ntfy_dat[self.client_ntfy_tok[int(uid)]]
        except:
            res = "failed"
        
        return res
    
    def set_notification_token(self, data):
        self.client_ntfy_tok[data['id']] = data['token_id']

    def send_notification(self, uid):
        
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title="Quinns Laundry House", #Notification Title
                    body=self.client_ntfy_dat[self.client_ntfy_tok[uid]]['content'], #Body
                ),
                data={
                    'title':self.client_ntfy_dat[self.client_ntfy_tok[uid]]['title'],
                    'type':self.client_ntfy_dat[self.client_ntfy_tok[uid]]['type'],
                    'msg':self.client_ntfy_dat[self.client_ntfy_tok[uid]]['msg'],
                    'id':self.client_ntfy_dat[self.client_ntfy_tok[uid]]['id'],
                },
                token=self.client_ntfy_tok[uid], #DEVICE_REGISTRATION_TOKEN
            )
            response = messaging.send(message)  
            #print("Successfully sent message:", response)
            #del self.client_ntfy_dat[self.client_ntfy_tok[uid]]
        except Exception as e:
            print("Error sending notification", e)            

    def create_user(self, arr):
        self.cur.execute(f"SELECT id from tbl_users WHERE user_name='{arr['uname']}'")
        res = self.cur.fetchone()
        if res == None:
            sql = f"INSERT INTO tbl_users (user_name,password,email,first_name,last_name,address,mobile_no,timestamp,status) VALUES (\'{arr['uname']}\', \'{arr['upass']}\', \'{arr['email']}\', \'{arr['fname']}\', \'{arr['lname']}\', \'{arr['addr']}\', \'{arr['mobile_no']}\', \'{self.get_datetime()}\', 'Pending') RETURNING id"
            try:
                self.cur.execute(sql)
                id = (self.cur.fetchone())['id']
                self.conn.commit()
                self.otp_list[id] = self.gen_rand_num_codes(4)
                res = id
            except:
                res = "invalid"
                self.conn.rollback()
        else:
            res = "exist"

        return res 
    
    def create_booking(self, arr):
        sql = f"INSERT INTO tbl_booking (user_id, schedule, mode, client, contact, pickup_loc, quantity, unit, timestamp, status) VALUES (\'{arr['uid']}\', \'{arr['sched']}\', \'{arr['mode']}\', \'{arr['client']}\', \'{arr['contact']}\', \'{arr['ploc']}\', '', '', \'{self.get_datetime()}\', 'Pending') RETURNING id"
        
        try:
            self.cur.execute(sql)
            id = (self.cur.fetchone())['id']
            sql = f"INSERT INTO tbl_threads (booking_id, timestamp, status) VALUES (\'{id}\', \'{self.get_datetime()}\', 'Open')"
            self.cur.execute(sql)
            i=1
            for key in arr:
                try:
                    if key != "sched_date":
                        tmp_arr = arr[key].split("-")
                        sql = f"INSERT INTO tbl_booking_addon (booking_id, service_id, prod_type) VALUES (\'{id}\', \'{tmp_arr[1]}\', \'{tmp_arr[0]}\')"
                        self.cur.execute(sql)
                except:
                    pass

            self.otp_list[id] = self.gen_rand_num_codes(4)
            res = id
            sql = f"INSERT INTO tbl_book_tracking (booking_id, sched) VALUES (\'{id}\', \'{arr['sched_date']}\')"
            self.cur.execute(sql)
            self.conn.commit()
            self.new_booking.insert(0, id)
        except:
            res = "invalid"
            self.conn.rollback()
            
        return res 
    
    def validate_user(self, arr):
        self.cur.execute(f"SELECT id, password, first_name, last_name, address, mobile_no, status from tbl_users WHERE user_name='{arr['uname']}' AND password='{arr['upass']}'")
        res = self.cur.fetchone()
        if res == None:
            res = "invalid"
        else:
            if res['status'] == "Pending":
                self.otp_list[res[id]] = self.gen_rand_num_codes(4)
            
        return res
    
    def request_reset_password(self, user_name):
        self.cur.execute(f"SELECT id from tbl_users WHERE user_name='{user_name}'")
        res = self.cur.fetchone()
        if res == None:
            res = "invalid"

        return res
    
    def reset_password(self, arr):
        if self.otp_list[arr['user_id']] == arr['otp']:
            try:
                res = self.cur.execute(f"UPDATE tbl_users SET password='{arr['upass']}' WHERE id={arr['user_id']}")
                self.conn.commit()
                self.otp_list.pop(arr['user_id'])
                res = "valid"
            except:
                res = "invalid"
                self.conn.rollback()
        else:
            res = "invalid"
        
        return res
    
    def check_otp(self, arr):
        if self.otp_list[arr['user_id']] == arr['otp']:
            try:
                res = self.cur.execute(f"UPDATE tbl_users SET status='Active' WHERE id={arr['user_id']}")
                self.conn.commit()
                self.otp_list.pop(arr['user_id'])
                res = "valid"
            except:
                res = "invalid"
                self.conn.rollback()
        else:
            res = "invalid"
        
        return res
    
    def retrieve_otp(self, arr):
        self.otp_list[arr['user_id']] = self.gen_rand_num_codes(4)
        res = self.otp_list[arr['user_id']]

        return res
    
    def get_user(self, id):
        if id == "all":
            self.cur.execute(f"SELECT * FROM tbl_users")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"SELECT * FROM tbl_users WHERE id={id}")
            res = self.cur.fetchone()

        return res
    
    def get_services(self, id):
        if id == "all":
            self.cur.execute(f"SELECT * FROM tbl_services ORDER BY id ASC")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"SELECT * FROM tbl_services WHERE id={id}")
            res = self.cur.fetchone()

        return res
    
    def get_addons(self, id):
        if id == "all":
            self.cur.execute(f"SELECT * FROM tbl_addons ORDER BY id ASC")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"SELECT * FROM tbl_addons WHERE id={id}")
            res = self.cur.fetchone()

        return res
    
    def get_user_points(self, id):
        
        self.cur.execute(f"SELECT SUM(amount) FROM tbl_pts_earned WHERE user_id={id}")
        res_earned = self.cur.fetchone()['sum']
        
        if res_earned == None:
            res_earned = 0

        self.cur.execute(f"SELECT SUM(amount) FROM tbl_pts_used WHERE user_id={id}")
        res_used = self.cur.fetchone()['sum']
        
        if res_used == None:
            res_used = 0
        
        return res_earned-res_used

    def get_earned_points(self, id):

        self.cur.execute(f"SELECT * FROM tbl_pts_earned WHERE user_id={id}")
        res = self.cur.fetchall()
        
        return res
    
    def get_points_history(self, id):

        #self.cur.execute(f"SELECT PE.*, PU.* FROM tbl_pts_earned PE INNER JOIN tbl_pts_used PU ON PE.user_id = PU.user_id WHERE PE.user_id={id} ORDER BY PE.timestamp DESC, PU.timestamp DESC")
        #res = self.cur.fetchall()

        self.cur.execute(f"SELECT * FROM tbl_pts_earned WHERE user_id={id} ORDER BY timestamp DESC")
        res1 = self.cur.fetchall()
        self.cur.execute(f"SELECT * FROM tbl_pts_used WHERE user_id={id} ORDER BY timestamp DESC")
        res2 = self.cur.fetchall()
        
        return [res1, res2]
    
    def get_usaged_points(self, id):

        self.cur.execute(f"SELECT * FROM tbl_pts_used WHERE user_id={id}")
        res = self.cur.fetchall()
        
        return res
    
    def get_points_reward(self, status):

        self.cur.execute(f"SELECT * FROM tbl_rewards WHERE status='{status}' ORDER BY pts_req DESC")
        res = self.cur.fetchall()
        
        return res
    
    # new mod

    def get_booking(self, arr):
        if arr['type'] == "all":
            self.cur.execute(f"SELECT * FROM tbl_booking WHERE user_id={arr['id']} ORDER BY {arr['sort']}")
        else:
            self.cur.execute(f"SELECT * FROM tbl_booking WHERE id={arr['id']} ORDER BY {arr['sort']}")

        res = self.cur.fetchall()
        
        return res
    
    def get_booking_services(self, arr):
        self.cur.execute(f"""SELECT BA.* FROM tbl_users U 
                         LEFT OUTER JOIN tbl_booking B ON B.user_id=U.id 
                         LEFT OUTER JOIN tbl_booking_addon BA ON BA.booking_id=B.id 
                         WHERE U.id={arr['id']} ORDER BY {arr['sort']}""")
        res = self.cur.fetchall()
        return res
    
    def get_booking_tracks(self, arr):
        self.cur.execute(f"SELECT * FROM tbl_book_tracking WHERE booking_id={arr['id']} ORDER BY {arr['sort']}")
        res = self.cur.fetchall()
        
        return res
    
    def get_booking_payments(self, arr):
        self.cur.execute(f"SELECT * FROM tbl_payments WHERE booking_id={arr['id']} ORDER BY {arr['sort']}")
        res = self.cur.fetchall()
        
        return res
    
    def set_update_field(self, arr):
        res = "valid"
        try:
            self.cur.execute(f"UPDATE {arr['table']} SET {arr['field']}='{arr['value']}' WHERE {arr['ref_field']}={arr['ref_value']}")
            self.conn.commit()
        except:
            res = "invalid"
            self.conn.rollback()
            
        return res
    
    # new mod
    
    def get_booked_services(self, arr):
        # self.cur.execute(f"""SELECT B.*, BA.*, U.id AS uid, U.email AS eadd FROM tbl_booking B 
        #                  LEFT OUTER JOIN tbl_users U ON B.user_id=U.id 
        #                  LEFT OUTER JOIN tbl_booking_addon BA ON B.id=BA.booking_id
        #                  """+arr['filter'])

        self.cur.execute(f"SELECT * FROM tbl_booking {arr['filter']}")
        res = self.cur.fetchall()
        
        return res
    
    def get_completed_bookings(self):
        self.cur.execute(f"""SELECT B.schedule, BT.sched FROM tbl_booking B 
                         LEFT OUTER JOIN tbl_book_tracking BT ON B.id=BT.booking_id 
                         WHERE B.status='Completed'""")
        res = self.cur.fetchall()
        
        return res
    
    def set_booked_threads(self, arr):
        res = "valid"
        try:
            sql = f"INSERT INTO tbl_thread_messages (thread_id, sender, message, timestamp) VALUES (\'{arr['thread_id']}\', \'{arr['user_id']}\', \'{arr['message']}\', \'{self.get_datetime()}\')"
            self.cur.execute(sql)
            self.conn.commit()
            self.new_msg.insert(0, arr['thread_id'])
        except:
            res = "invalid"
        
        return res
    
    def get_booked_threads(self, arr):
        self.cur.execute(f"SELECT T.*, TM.* FROM tbl_threads T LEFT OUTER JOIN tbl_booking B ON B.id=T.booking_id LEFT OUTER JOIN tbl_thread_messages TM ON T.id=TM.thread_id "+arr['filter']+" ORDER BY TM.timestamp ASC")
        res = self.cur.fetchall()
        
        return res
    
    # Admin queries

    def get_shop(self):
        self.cur.execute(f"SELECT * FROM tbl_shop")
        res = self.cur.fetchone()

        return res
    
    def mod_tbl_shop(self, arr):
        res = "valid"
        try:    
            self.cur.execute(f"""UPDATE tbl_shop SET 
                            shop_name = '{arr['shop_name']}',
                            address = '{arr['address']}',
                            contact = '{arr['contact']}',
                            fb_page = '{arr['fb_page']}',
                            logistics = '{arr['logistics']}',
                            free_threshold = '{arr['free_threshold']}',
                            sunday = '{arr['sunday']}',
                            sun_stat = '{arr['sun_stat']}',
                            monday = '{arr['monday']}',
                            mon_stat = '{arr['mon_stat']}',
                            tuesday = '{arr['tuesday']}',
                            tue_stat = '{arr['tue_stat']}',
                            wednesday = '{arr['wednesday']}',
                            wed_stat = '{arr['wed_stat']}',
                            thursday = '{arr['thursday']}',
                            thu_stat = '{arr['thu_stat']}',
                            friday = '{arr['friday']}',
                            fri_stat = '{arr['fri_stat']}',
                            saturday = '{arr['saturday']}',
                            sat_stat = '{arr['sat_stat']}'
                            """)
            self.conn.commit()
        except:
            res = "invalid"
            self.conn.rollback()

        return res

    def get_threads_admin(self):
        self.cur.execute(f"""
                         SELECT DISTINCT T.id, T.*, U.first_name, U.last_name FROM tbl_threads T 
                         LEFT OUTER JOIN tbl_thread_messages TM ON T.id=TM.thread_id 
                         LEFT OUTER JOIN tbl_users U ON CAST(TM.sender AS INT)=U.id 
                         WHERE T.status='Open' AND sender<>'0' ORDER BY T.timestamp DESC""")
        res = self.cur.fetchall()

        return res
    
    def get_thread_messages_admin(self, id):
        self.cur.execute(f"""
                         SELECT DISTINCT TM.* FROM tbl_threads T 
                         LEFT OUTER JOIN tbl_thread_messages TM ON T.id=TM.thread_id 
                         LEFT OUTER JOIN tbl_users U ON CAST(TM.sender AS INT)=U.id 
                         WHERE T.id={id} AND T.status='Open' ORDER BY TM.timestamp DESC""")
        res = self.cur.fetchall()

        return res
    
    def set_thread_reply_admin(self, arr):
        res = "valid"
        try:
            sql = f"INSERT INTO tbl_thread_messages (thread_id, sender, message, timestamp) VALUES (\'{arr['thread_id']}\', \'{arr['sender_id']}\', \'{arr['message']}\', \'{self.get_datetime()}\')"
            self.cur.execute(sql)
            self.conn.commit()
        except:
            res = "invalid"

        if res == "valid":
            
            try:
                ntfy_msg = ""
                if arr['message'] != "":
                    ntfy_msg = arr['message']

                if ntfy_msg != "":
                    self.cur.execute(f"""
                         SELECT DISTINCT B.user_id FROM tbl_threads T 
                         LEFT OUTER JOIN tbl_booking B ON T.booking_id=B.id 
                         WHERE T.id={arr['thread_id']}""")
                    res = self.cur.fetchone()
                    user_id = res['user_id']
                    
                    pending_ntfy = 0
                    try:
                        pending_ntfy = self.client_ntfy_dat[self.client_ntfy_tok[user_id]]['content']
                    except:
                        pass
                    
                    if pending_ntfy == 0:
                        self.client_ntfy_dat[self.client_ntfy_tok[user_id]] = {}
                        self.client_ntfy_dat[self.client_ntfy_tok[user_id]]['content'] = "Admin replied to your thread"
                        self.client_ntfy_dat[self.client_ntfy_tok[user_id]]['type'] = "message"
                        self.client_ntfy_dat[self.client_ntfy_tok[user_id]]['title'] = "Admin Reply"
                        self.client_ntfy_dat[self.client_ntfy_tok[user_id]]['msg'] = ntfy_msg
                        self.client_ntfy_dat[self.client_ntfy_tok[user_id]]['id'] = str(arr['thread_id'])
                        self.send_notification(user_id)

            except:
                pass

        return res
    
    def mod_tbl_threads(self, act, arr):
        if act == "Delete":
            res = "valid"
            try:
                if arr['timestamp'] == "": 
                    self.cur.execute(f"DELETE FROM tbl_threads WHERE id = {arr['thread_id']}")
                    self.cur.execute(f"DELETE FROM tbl_thread_messages TM USING tbl_threads T WHERE T.booking_id = {arr['thread_id']} AND TM.thread_id = T.id")
                else:
                    if arr['sender'] == '0':
                        self.cur.execute(f"DELETE FROM tbl_thread_messages WHERE thread_id = {arr['thread_id']} AND timestamp = '{arr['timestamp']}'")
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()

        return res
    
    def get_booking_pack(self, id):
        self.cur.execute(f"SELECT * FROM tbl_booking_addon WHERE booking_id={id}")
        res = self.cur.fetchall()

        return res

    def mod_tbl_bookings(self, act, arr):
        if act == "Delete":
            res = "valid"
            try:
                self.cur.execute(f"DELETE FROM tbl_booking WHERE id = '{arr['booking_id']}'")
                self.cur.execute(f"DELETE FROM tbl_booking_addon WHERE booking_id = '{arr['booking_id']}'")
                self.cur.execute(f"DELETE FROM tbl_book_tracking WHERE booking_id = '{arr['booking_id']}'")
                self.cur.execute(f"DELETE FROM tbl_payments WHERE booking_id = '{arr['booking_id']}'")
                self.cur.execute(f"DELETE FROM tbl_threads WHERE booking_id = '{arr['booking_id']}'")
                self.cur.execute(f"DELETE FROM tbl_thread_messages TM USING tbl_threads T WHERE T.booking_id = '{arr['booking_id']}' AND TM.thread_id = T.id")
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()
        elif act == "Update": 
            res = "valid"
            try:
                self.cur.execute(f"UPDATE tbl_booking SET quantity='{arr['quantity']}', unit='{arr['unit']}', status='{arr['status']}', logistics_fee='{arr['logistics_fee']}' WHERE id={arr['booking_id']}")
                stat_arr = {'Pending':'sched','Accepted':'accepted','Pickup':'pickup','Drop Off':'drop_off','Arrived':'arrived','Ongoing':'processing','Delivery':'outgoing','To Receive':'outgoing','Completed':'completed','Cancelled':'cancelled'}
                self.cur.execute(f"UPDATE tbl_book_tracking SET {stat_arr[arr['status']]}='{self.get_datetime()}' WHERE booking_id={arr['booking_id']}")
                if arr['status'] == "Delivery" or arr['status'] == "To Receive":
                    self.cur.execute(f"SELECT id from tbl_payments WHERE booking_id='{arr['booking_id']}'")
                    res = self.cur.fetchone()
                    if res == None:
                        sql = f"INSERT INTO tbl_payments (booking_id,mode,ref_num,amount,status,timestamp,add_charges,description) VALUES (\'{arr['booking_id']}\','','',0,'','',0,'')"
                        self.cur.execute(sql)
                    #print((self.cur.fetchone())['id'])
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()

            try:
                ntfy_msg = ""
                if arr['status'] != "" and arr['status'] != "Pending":
                    ntfy_msg = arr['status']

                if ntfy_msg != "":
                    pending_ntfy = 0
                    try:
                        pending_ntfy = self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['content']
                    except:
                        pass
                        
                    if pending_ntfy == 0:
                        self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]] = {}
                        self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['content'] = f"Booking {self.format_std_code("QLH", str(arr['booking_id']), 6)} update."
                        self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['type'] = "booking"
                        self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['title'] = "Booking Update" #f"Booking {self.format_std_code("QLH", str(arr['booking_id']), 6)} Status: {arr['status']}"
                        self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['msg'] = ntfy_msg
                        self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['id'] = str(arr['booking_id'])
                        self.send_notification(arr['uid'])

            except:
                pass
        
        return res
    
    def get_rewards_list(self):
        self.cur.execute(f"SELECT * FROM tbl_rewards ORDER BY pts_req DESC")
        res = self.cur.fetchall()
        
        return res
    
    def mod_tbl_rewards(self, act, arr):
        if act == "Add":
            try:
                sql = f"INSERT INTO tbl_rewards (title, description, pts_req, status) VALUES (\'{arr['title']}\', \'{arr['description']}\', \'{arr['pts_req']}\', \'{arr['status']}\') RETURNING id"
                self.cur.execute(sql)
                self.conn.commit()
                res = (self.cur.fetchone())['id']
            except:
                res = "invalid"
        elif act == "Update":
            res = "valid"
            try:
                self.cur.execute(f"UPDATE tbl_rewards SET title='{arr['title']}', description='{arr['description']}', pts_req='{arr['pts_req']}', status='{arr['status']}' WHERE id={arr['id']}")
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()
        elif act == "Delete":
            res = "valid"
            try:
                self.cur.execute(f"DELETE FROM tbl_rewards WHERE id={arr['id']}")
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()

        return res
    
    def get_billing_payments(self, id):
        if id == "all":
            self.cur.execute(f"""SELECT P.*, B.user_id, B.client, B.contact, B.pickup_loc, B.logistics_fee FROM tbl_payments P 
                            LEFT OUTER JOIN tbl_booking B ON P.booking_id=B.id 
                            ORDER BY P.id DESC""")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"""SELECT BA.*, B.quantity, B.unit FROM tbl_payments P 
                            LEFT OUTER JOIN tbl_booking B ON P.booking_id=B.id 
                            LEFT OUTER JOIN tbl_booking_addon BA ON P.booking_id=BA.booking_id 
                            WHERE P.id={id} ORDER BY P.timestamp DESC""")
            res = self.cur.fetchall()
        
        return res
    
    def mod_tbl_services(self, act, arr):
        if act == "Update":
            res = "valid"
            try:
                self.cur.execute(f"UPDATE tbl_services SET sub_title='{arr['sub_title']}', description='{arr['description']}', price='{arr['price']}', quantity='{arr['quantity']}', unit='{arr['unit']}', status='{arr['status']}' WHERE id={arr['id']}")
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()

        return res
    
    def mod_tbl_addons(self, act, arr):
        if act == "Update":
            res = "valid"
            try:
                self.cur.execute(f"UPDATE tbl_addons SET title='{arr['title']}', description='{arr['description']}', price='{arr['price']}', status='{arr['status']}' WHERE id={arr['id']}")
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()

        return res
    
    def mod_tbl_billings(self, act, arr):
        if act == "Update":
            res = "valid"
            try:
                self.cur.execute(f"UPDATE tbl_payments SET mode='{arr['mode']}', ref_num='{arr['ref_num']}', amount='{arr['amount']}', status='{arr['status']}' WHERE id={arr['id']}")
                if arr['payment_status'] == "Unpaid":
                    if arr['mode'] == "Points":
                        sql = f"INSERT INTO tbl_pts_used (user_id,amount,benefit,timestamp) VALUES (\'{arr['uid']}\',\'{arr['amount']}\','Payment for booking {self.format_std_code("QLH", str(arr['booking_id']), 6)}',\'{self.get_datetime()}\')"
                        self.cur.execute(sql)
                    if arr['status'] == "Paid":
                        if arr['points'] != '' and arr['points'] != '0':
                            sql = f"INSERT INTO tbl_pts_earned (user_id,amount,source,timestamp) VALUES (\'{arr['uid']}\',\'{arr['points']}\',\'{arr['description']}\',\'{self.get_datetime()}\')"
                            #sql = f"INSERT INTO tbl_pts_used (user_id,amount,benefit,timestamp) VALUES (\'{arr['uid']}\',\'{arr['points']}\',\'{arr['description']}\',\'{self.get_datetime()}\')"
                            self.cur.execute(sql)
                        
                        self.cur.execute(f"UPDATE tbl_booking SET status='Completed' WHERE id={arr['booking_id']}")

                        pending_ntfy = 0
                        try:
                            pending_ntfy = self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['content']
                        except:
                            pass
                        
                        if pending_ntfy == 0:
                            self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]] = {}
                            self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['content'] = f"Booking {self.format_std_code("QLH", str(arr['booking_id']), 6)} update."
                            self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['type'] = "booking"
                            self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['title'] = "Booking Update"
                            self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['msg'] = "Completed"
                            self.client_ntfy_dat[self.client_ntfy_tok[arr['uid']]]['id'] = str(arr['booking_id'])
                            self.send_notification(arr['uid'])
                        
                self.conn.commit()
            except:
                res = "invalid"
                self.conn.rollback()

        return res
    
    # Admin queries