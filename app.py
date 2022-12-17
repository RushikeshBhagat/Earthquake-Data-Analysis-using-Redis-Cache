#UTA ID:1001911486
#Name: Rushikesh Mahesh Bhagat

from flask import Flask, flash, render_template, request, redirect

import pyodbc
import csv

from datetime import datetime
import haversine as hs
import requests

from settings_template import server, database, username, password, driver, mapQuest_key, mapQuest_url, myRedisHostname, myRedisPassword

import time
import redis
import pickle
connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

try:
    redisClient = redis.StrictRedis(host=myRedisHostname, port=6380,
                        password=myRedisPassword, ssl=True)

    resultred = redisClient.ping()
    print("Ping returned : " + str(resultred))
except Exception as e:
        print(e,"Error connecting to Redis")

app = Flask(__name__)


def select_query(query):
    list_result=[]
    try:
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(query)
        
        list_result = cursor.fetchall()

    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    return list_result

@app.route("/", methods=['GET','POST'])
@app.route("/index", methods=['GET','POST'])
def index():
    if request.method == 'POST':
        try:
            if 'clear_cache_button' in request.form:
                redisClient.flushall()
                return render_template('index.html')

            if 'n_times_31' in request.form:
                n_times = request.form["n_times_31"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    for i in range(0,n_times):
                        search_query = "SELECT * FROM all_month WHERE 1 = 1 "
                        list_result31 = select_query(search_query)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    statement_31 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows31 = f"The total count of earthquakes occured is {(len(list_result31))}"

                return render_template('index.html',count_rows31=count_rows31,statement_31=statement_31)
            
            if 'n_times_32' in request.form:
                n_times = request.form["n_times_32"]
                from_date = request.form["from_date_32"]
                to_date = request.form["to_date_32"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    for i in range(0,n_times):
                        search_query = "SELECT * FROM all_month WHERE 1 = 1 "
                        if len(from_date) !=0  and len(to_date) !=0 :
                            search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                        print("search",search_query)
                        list_result32 = select_query(search_query)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    statement_32 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows32 = f"The total count of earthquakes occured is {(len(list_result32))}  from {from_date} to {to_date}."

                return render_template('index.html',count_rows32=count_rows32,statement_32=statement_32)


            
            if 'n_times_33' in request.form:
                n_times = request.form["n_times_33"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    redis_key = "restricted_query"
                    search_query = "SELECT * FROM all_month WHERE 1 = 1 "

                    for i in range(0,n_times):
                        redis_key_exists = redisClient.exists(redis_key)

                        if not redis_key_exists:
                            list_result33 = select_query(search_query)
                            redisClient.set(redis_key,pickle.dumps(list_result33))
                        else:
                            list_result33 = redisClient.get(redis_key)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    list_result33 = pickle.loads(list_result33)

                    statement_33 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows33 = f"The total count of earthquakes occured is {(len(list_result33))}"

                return render_template('index.html',count_rows33=count_rows33,statement_33=statement_33,scroll_33="scroll_33")
            

            if 'n_times_34' in request.form:
                n_times = request.form["n_times_34"]
                from_date = request.form["from_date_34"]
                to_date = request.form["to_date_34"]
                if int(n_times) >=0 and int(n_times) <= 1000:
                    n_times = int(n_times)
                    init_time = time.time()
                    redis_key = "unrestricted_query"
                    search_query = "SELECT * FROM all_month WHERE 1 = 1 "
                    if len(from_date) !=0  and len(to_date) !=0 :
                            search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"

                    for i in range(0,n_times):
                        redis_key_exists = redisClient.exists(redis_key)

                        if not redis_key_exists:
                            list_result34 = select_query(search_query)
                            redisClient.set(redis_key,pickle.dumps(list_result34))
                        else:
                            list_result34 = redisClient.get(redis_key)
                    final_time = time.time()
                    exec_time = final_time - init_time
                    list_result34 = pickle.loads(list_result34)

                    statement_34 = f"The Execution time is  {exec_time}  seconds for running this query {n_times} times."
                    count_rows34 = f"The total count of earthquakes occured is {(len(list_result34))}  from {from_date} to {to_date}."

                return render_template('index.html',count_rows34=count_rows34,statement_34=statement_34,scroll_34="scroll_34")


            if 'search_mag' in request.form:
                magnitude = request.form["search_mag"]
                from_date = request.form["search_from_date"]
                to_date = request.form["search_to_date"]

                search_query = "SELECT * FROM all_month WHERE 1 = 1 "
                if len(magnitude)!=0:
                    magnitude = float(magnitude)
                    search_query+= "AND mag > " + str(magnitude)
                if len(from_date) !=0  and len(to_date) !=0 :
                    search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                list_result1 = select_query(search_query)
                count_rows = f"The count of earthquakes occured with magnitude greather than {magnitude} is {(len(list_result1))} from {from_date} to {to_date}"
                
                t_headings1 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('index.html',count_rows=count_rows,t_headings1=t_headings1,list_result1=list_result1,scroll1="scroll1")
            
            if 'dist_dist' in request.form:
                location = request.form["dist_loc"]
                latitude = request.form["dist_lat"]
                longitude = request.form["dist_long"]
                from_date = request.form["dist_from_date"]
                to_date = request.form["dist_to_date"]
                if len(location)!=0:
                    main_url = mapQuest_url+mapQuest_key+'&location='+location
                    print(main_url)

                    location_data = requests.get(main_url).json()['results'][0]['locations'][0]['latLng']
                    print("location_data=",location_data)
                    latitude = location_data['lat']
                    longitude = location_data['lng']

                distance = request.form["dist_dist"]
                if len(distance)!=0:
                    distance = float(distance)

                search_query = "SELECT * FROM all_month WHERE 1 = 1 "
                if len(from_date) !=0  and len(to_date) !=0 :
                    search_query+=" AND time between '" + str(from_date) + "' AND '" + str(to_date) + "'"
                search_query+=" ORDER By mag DESC"
                list_result2 = select_query(search_query)
                location1 = (float(latitude),float(longitude))
                    
                list_result2_updated = []
                for item in list_result2:
                    location2 = (float(item[1]),float(item[2]))
                    actual_diff = hs.haversine(location1,location2)
                    if distance >= actual_diff:
                        list_result2_updated.append(item)
                
                count_rows2 = f"The count of earthquakes from {location} lat= {latitude} and lng= {longitude} within {distance} km is {len(list_result2_updated)} from {from_date} to {to_date}"
                
                t_headings2 = ["time", "latitude","longitude", "depth","mag","magType","nst","gap","dmin","rms","net","id","updated","place","type","horizontalError","depthError","magError","magNst","status","locationSource","magSource"]

                return render_template('index.html',count_rows2=count_rows2,t_headings2=t_headings2,list_result2_updated=list_result2_updated,scroll2="scroll2")
            


        except Exception as e:
            print(e,"Error has occured")
            

    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)

