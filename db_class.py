import psycopg2
import random
from datetime import datetime

x = datetime.now()
print(x.strftime("%c"))

class db_strg:
    otp_list = {}

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
            #self.conn = psycopg2.connect(database="quinns_laundry_db", user = "postgres", password = "XDm4y4143", host = "127.0.0.1", port = "5432")
            self.cur = self.conn.cursor()
            #conn.close()
            try:
                self.cur.execute('''CREATE TABLE tbl_users (id SERIAL PRIMARY KEY, user_name TEXT NOT NULL, password TEXT NOT NULL, email TEXT, first_name TEXT NOT NULL, last_name TEXT NOT NULL, address TEXT, mobile_no TEXT, timestamp TEXT, status TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_booking (id SERIAL PRIMARY KEY, user_id INT, schedule TEXT, mode TEXT, client TEXT, contact TEXT, pickup_loc TEXT, quantity TEXT, unit TEXT, timestamp TEXT, status TEXT);''')
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
                self.cur.execute('''CREATE TABLE tbl_payments (id SERIAL PRIMARY KEY, booking_id INT, mode TEXT, ref_num TEXT, amount NUMERIC, status TEXT, timestamp TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_rewards (id SERIAL PRIMARY KEY, description TEXT, pts_req INT, status TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_qouta (date TEXT PRIMARY KEY, day TEXT, min_qouta INT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_pts_earned (user_id INT, amout INT, source TEXT, timestamp TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback()

            try:
                self.cur.execute('''CREATE TABLE tbl_pts_earned (user_id INT, amout INT, benefit TEXT, timestamp TEXT);''')
                self.conn.commit()
            except:
                self.conn.rollback() 

        except:
            print("Database connection error!")  

    def gen_rand_num_codes(self, len):
        i=0
        code = ""
        while i < len:
            code += str(random.randint(0, 9))
            i += 1
            
        return code   

    def get_datetime(self):
        dt_now = datetime.now()
        dt_now = dt_now.strftime("%Y-%m-%d %H:%M:%S")

        return dt_now       

    def create_user(self, arr):
        sql = f"INSERT INTO tbl_users (user_name,password,email,first_name,last_name,address,mobile_no,timestamp,status) VALUES (\'{arr['uname']}\', \'{arr['upass']}\', \'{arr['email']}\', \'{arr['fname']}\', \'{arr['lname']}\', \'{arr['addr']}\', \'{arr['mobile_no']}\', \'{self.get_datetime()}\', 'Pending') RETURNING id"
        try:
            self.cur.execute(sql)
            id = (self.cur.fetchone())[0]
            self.conn.commit()
            self.otp_list[id] = self.gen_rand_num_codes(4)
            res = id
        except:
            res = "invalid"
            self.conn.rollback()
            
        return res 
    
    def create_booking(self, arr):
        sql = f"INSERT INTO tbl_booking (user_id, schedule, mode, client, contact, pickup_loc, quantity, unit, timestamp, status) VALUES (\'{arr['uid']}\', \'{arr['sched']}\', \'{arr['mode']}\', \'{arr['client']}\', \'{arr['contact']}\', \'{arr['ploc']}\', '', '', \'{self.get_datetime()}\', 'Pending') RETURNING id"
        
        try:
            self.cur.execute(sql)
            id = (self.cur.fetchone())[0]
            sql = f"INSERT INTO tbl_threads (booking_id, timestamp, status) VALUES (\'{id}\', \'{self.get_datetime()}\', 'Open')"
            self.cur.execute(sql)
            i=1
            for key in arr:
                try:
                    tmp_arr = arr[key].split("-")
                    sql = f"INSERT INTO tbl_booking_addon (booking_id, service_id, prod_type) VALUES (\'{id}\', \'{tmp_arr[1]}\', \'{tmp_arr[0]}\')"
                    self.cur.execute(sql)
                except:
                    pass

            self.conn.commit()
            self.otp_list[id] = self.gen_rand_num_codes(4)
            res = id
        except:
            res = "invalid"
            self.conn.rollback()
            
        return res 
    
    def validate_user(self, arr):
        self.cur.execute(f"SELECT id, first_name, last_name, address, mobile_no, status from tbl_users WHERE user_name='{arr['uname']}' AND password='{arr['upass']}'")
        res = self.cur.fetchone()
        if res == None:
            res = "invalid"
        else:
            if res[4] == "Pending":
                self.otp_list[res[0]] = self.gen_rand_num_codes(4)
        
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
        res = self.otp_list[arr['user_id']]

        return res
    
    def get_user(self, cnd):
        if cnd == "all":
            self.cur.execute(f"SELECT * FROM tbl_users")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"SELECT * FROM tbl_users WHERE "+cnd)
            res = self.cur.fetchone()

        return res
    
    def get_services(self, cnd):
        if cnd == "all":
            self.cur.execute(f"SELECT * FROM tbl_services")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"SELECT * FROM tbl_services WHERE "+cnd)
            res = self.cur.fetchone()

        return res
    
    def get_addons(self, cnd):
        if cnd == "all":
            self.cur.execute(f"SELECT * FROM tbl_addons")
            res = self.cur.fetchall()
        else:
            self.cur.execute(f"SELECT * FROM tbl_addons WHERE "+cnd)
            res = self.cur.fetchone()

        return res
    
    def get_booked_services(self, arr):
        self.cur.execute(f"""SELECT B.*, BA.*, U.id AS uid, U.email AS eadd FROM tbl_booking B 
                         LEFT OUTER JOIN tbl_users U ON B.user_id=U.id 
                         LEFT OUTER JOIN tbl_booking_addon BA ON B.id=BA.booking_id
                         """+arr['filter'])
        res = self.cur.fetchall()
        
        return res
    
    def set_booked_threads(self, arr):
        res = "valid"
        try:
            sql = f"INSERT INTO tbl_thread_messages (thread_id, sender, message, timestamp) VALUES (\'{arr['thread_id']}\', \'{arr['user_id']}\', \'{arr['message']}\', \'{self.get_datetime()}\')"
            self.cur.execute(sql)
            self.conn.commit()
        except:
            res = "invalid"
        
        return res
    
    def get_booked_threads(self, arr):
        self.cur.execute(f"SELECT T.*, TM.* FROM tbl_threads T LEFT OUTER JOIN tbl_booking B ON B.id=T.booking_id LEFT OUTER JOIN tbl_thread_messages TM ON T.id=TM.thread_id "+arr['filter'])
        res = self.cur.fetchall()
        
        return res
    
    def update_client_booked(self, arr):
        res = "valid"
        try:
            self.cur.execute(f"UPDATE tbl_booking SET status='{arr['status']}' WHERE id={arr['booking_id']}")
            self.conn.commit()
        except:
            res = "invalid"
            self.conn.rollback()
            
        return res