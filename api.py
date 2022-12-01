from flask import Flask,jsonify,render_template,request
import sqlite3
import json
import uuid
from flask import abort
from flask import Response
import time



app = Flask(__name__)

#########################################################################################################################################################################################
# DB CONNECTION
def get_db_connection():
    conn = sqlite3.connect('/home/ubuntu/Tarea3/Database/Emergentes.db')
    return conn



#########################################################################################################################################################################################
# CREATE MODELS ENDPOINTS

@app.route("/createcompany", methods=["POST"])
def createcompany():
    connection = sqlite3.connect('/home/ubuntu/Tarea3/Database/Emergentes.db')
    cur = connection.cursor()
    data = request.json
    jsonnuevo=list()
    jsonnuevo.append({"companyname":data['companyname'],"company_api_key":str(uuid.uuid4())})
    print(jsonnuevo[0]["company_api_key"])
    cur.execute("INSERT INTO Company (company_name, company_api_key ) VALUES (?, ?)",
            (jsonnuevo[0]["companyname"], jsonnuevo[0]["company_api_key"])
            )
    connection.commit()
    connection.close()
    return jsonify(jsonnuevo,{"status":201})


@app.route("/createlocation", methods=["POST"])
def createlocation():
    connection = sqlite3.connect('/home/ubuntu/Tarea3/Database/Emergentes.db')
    cur = connection.cursor()
    data = request.json
    jsonnuevo=list()
    jsonnuevo.append({"companyid":data['companyid'],"locationname":data['locationname'],"locationcountry":data['locationcountry'],"locationcity":data['locationcity'],"locationmeta":data['locationmeta']})
    cur.execute("INSERT INTO Location (company_id, location_name,location_country,location_city,location_meta ) VALUES (?, ?,?,?,?)",
            (jsonnuevo[0]["companyid"], jsonnuevo[0]["locationname"], jsonnuevo[0]["locationcountry"], jsonnuevo[0]["locationcity"], jsonnuevo[0]["locationmeta"])
            )
    connection.commit()
    connection.close()
    return jsonify(jsonnuevo,{"status":201})

@app.route("/createsensor", methods=["POST"])
def createsensor():
    connection = sqlite3.connect('/home/ubuntu/Tarea3/Database/Emergentes.db')
    cur = connection.cursor()
    data = request.json
    jsonnuevo=list()
    jsonnuevo.append({"locationid":data['locationid'],"sensorname":data['sensorname'],"sensorcategory":data['sensorcategory'],"sensormeta":data['sensormeta'],"sensorapikey":str(uuid.uuid4())})
    #jsonnuevo=json.dumps(jsonnuevo)
    #print(jsonnuevo[0]["company_api_key"])
    cur.execute("INSERT INTO Sensor (location_id, sensor_name,sensor_category ,sensor_meta , sensor_api_key ) VALUES (?,?,?,?,?)",
            (jsonnuevo[0]["locationid"], jsonnuevo[0]["sensorname"], jsonnuevo[0]["sensorcategory"], jsonnuevo[0]["sensormeta"], jsonnuevo[0]["sensorapikey"])
            )
    connection.commit()
    connection.close()
    return jsonify(jsonnuevo,{"status":201})



#########################################################################################################################################################################################
# MODEL LOCATION ENDPOINTS

@app.route("/get_all_locations")
def get_all_locations():
    conn = get_db_connection()
    posts = conn.execute('SELECT * FROM Location').fetchall()
    conn.close()
    jsondata=list()

    if posts.__len__()==0:
        return jsonify({"message":"Loaction empty","status":204})  
    else:
        for i in posts:
            jsondata.append({"Location_id":i[0],"Company_id":i[1],"Location_name":i[2],"Location_country":i[3],"Location_city":i[4],"Location_meta":i[5]})
        return jsonify(jsondata,{"status":201})

@app.route("/get_location_company", methods=["GET"])
def get_location_company():
    company=request.headers.get('company_api_key')
    conn = get_db_connection()
    posts = conn.execute("SELECT * FROM Location,Company where Company.company_api_key=? and Location.company_id=Company.ID ",(company,)).fetchall()
    conn.close()
    jsondata=list()

    if posts.__len__()==0:
        return jsonify({"message":"No Locations associated to this Company api key ","status":204})  
    else:
        for i in posts:
            jsondata.append({"Location_id":i[0],"Company_id":i[1],"Location_name":i[2],"Location_country":i[3],"Location_city":i[4],"Location_meta":i[5]})
        return jsonify(jsondata,{"status":201})

@app.route("/edit_location_company",methods=["PUT"])
def edit_location_company():
    data = request.json
    company=data['company_api_key']
    location=data['location_id']
    conn=get_db_connection()
    posts=conn.execute("SELECT * FROM Location,Company where Company.company_api_key=? and Location.company_id=Company.ID and Location.location_id=?",(company,location)).fetchall()
    conn.close()
    jsondata=list()
    if posts.__len__()==0:
        return jsonify({"message":"No Locations associated to this Company api key ","status":204})
    else:
        conn=get_db_connection()
        conn.execute("UPDATE Location SET location_name=?,location_country=?,location_city=?,location_meta=? WHERE location_id=?",(data['location_name'],data['location_country'],data['location_city'],data['location_meta'],location))
        conn.commit()
        conn.close()
        return jsonify({"message":"Location updated","status":201})

@app.route("/delete_location_company",methods=["DELETE"])
def delete_location_company():
    data = request.json
    company=data['company_api_key']
    location=data['location_id']
    conn=get_db_connection()
    posts=conn.execute("SELECT * FROM Location,Company where Company.company_api_key=? and Location.company_id=Company.ID and Location.location_id=?",(company,location)).fetchall()
    conn.close()
    jsondata=list()
    if posts.__len__()==0:
        return jsonify({"message":"No Locations associated to this Company api key ","status":204})
    else:
        conn=get_db_connection()
        conn.execute("DELETE FROM Location WHERE location_id=?",(location,))
        conn.commit()
        conn.close()
        return jsonify({"message":"Location deleted","status":201})

    
#########################################################################################################################################################################################
# MODEL SENSOR ENDPOINTS

@app.route("/get_all_sensors")
def get_all_sensors():
    conn = get_db_connection()
    posts = conn.execute('SELECT * FROM Sensor').fetchall()
    conn.close()
    jsondata=list()

    if posts.__len__()==0:
        return jsonify({"message":"Sensor empty","status":204})  
    else:
        for i in posts:
            jsondata.append({"Sensor_id":i[0],"Location_id":i[1],"Sensor_name":i[2],"Sensor_category":i[3],"Sensor_meta":i[4],"Sensor_api_key":i[5]})
        return jsonify(jsondata,{"status":201})

@app.route("/get_sensor_location", methods=["GET"])
def get_sensor_location():
    company=request.headers.get('company_api_key')
    conn = get_db_connection()
    posts = conn.execute("SELECT * FROM Sensor,Location,Company where Location.company_id=Company.ID and Sensor.location_id=Location.location_id and Company.company_api_key=?",(company,)).fetchall()
    conn.close()
    jsondata=list()

    if posts.__len__()==0:
        return jsonify({"message":"No Sensors associated to this Location api key ","status":204})  
    else:
        for i in posts:
            jsondata.append({"Sensor_id":i[0],"Location_id":i[1],"Sensor_name":i[2],"Sensor_category":i[3],"Sensor_meta":i[4],"Sensor_api_key":i[5]})
        return jsonify(jsondata,{"status":201})

@app.route("/edit_sensor_location",methods=["PUT"])
def edit_sensor_location():
    data = request.json
    company=data['company_api_key']
    sensor=data['sensor_id']
    conn=get_db_connection()
    posts=conn.execute("SELECT * FROM Sensor,Location,Company where Location.company_id=Company.ID and Sensor.location_id=Location.location_id and Company.company_api_key=? and Sensor.sensor_id=?",(company,sensor)).fetchall()
    conn.close()
    jsondata=list()
    if posts.__len__()==0:
        return jsonify({"message":"No Sensors associated to this Location api key ","status":204})
    else:
        conn=get_db_connection()
        conn.execute("UPDATE Sensor SET sensor_name=?,sensor_category=?,sensor_meta=? WHERE sensor_id=?",(data['sensor_name'],data['sensor_category'],data['sensor_meta'],sensor))
        conn.commit()
        conn.close()
        return jsonify({"message":"Sensor updated","status":201})

@app.route("/delete_sensor_location",methods=["DELETE"])
def delete_sensor_location():
    data = request.json
    company=data['company_api_key']
    sensor=data['sensor_id']
    conn=get_db_connection()
    posts=conn.execute("SELECT * FROM Sensor,Location,Company where Location.company_id=Company.ID and Sensor.location_id=Location.location_id and Company.company_api_key=? and Sensor.sensor_id=?",(company,sensor)).fetchall()
    conn.close()
    jsondata=list()
    if posts.__len__()==0:
        return jsonify({"message":"No Sensors associated to this Location api key ","status":204})
    else:
        conn=get_db_connection()
        conn.execute("DELETE FROM Sensor WHERE sensor_id=?",(sensor,))
        conn.commit()
        conn.close()
        return jsonify({"message":"Sensor deleted","status":201})


#########################################################################################################################################################################################
# SENSOR DATA ENDPOINTS


@app.route("/insert_sensor_data", methods=["POST"])
def insert_sensor_data():
    connection = sqlite3.connect('/home/ubuntu/Tarea3/Database/Emergentes.db')
    cur = connection.cursor()
    data = request.json
    jsonnuevo=list()
    jsonnuevo.append({"sensorapikey":data['sensorapikey'],"json_data":data['json_data']})
    finded=False
    conn = get_db_connection()
    posts = conn.execute('SELECT * FROM Sensor').fetchall()
    conn.close()
    jsondata=list()
    for i in posts:
        jsondata.append({"sensorapikey":i[5]})
    
    for i in jsondata:
        if i["sensorapikey"]==jsonnuevo[0]["sensorapikey"]:
            finded=True
    
    if finded==True:
        if jsonnuevo[0]['json_data'].__len__()>1:
            for i in jsonnuevo[0]['json_data']:
                t = time.time()
                ml = int(t * 1000)
                
                
                cur.execute("INSERT INTO Sensor_Data (api_key, medicion,fecha ) VALUES (?,?,?)",
                        (jsonnuevo[0]["sensorapikey"], i,ml)
                        )
                
        else:
            t = time.time()
            ml = int(t * 1000)
            
            cur.execute("INSERT INTO Sensor_Data (api_key, medicion,fecha ) VALUES (?,?,?)",
                    (jsonnuevo[0]["sensorapikey"], jsonnuevo[0]["json_data"][0],ml)
                    )

    else:
        abort(400)
    connection.commit()
    connection.close()
    return jsonify(jsonnuevo,{"status":201})



@app.route("/get_sensor_data", methods=["GET"])
def get_sensor_data():
    company=request.headers.get('company_api_key')
    desde=request.headers.get('from')
    hasta=request.headers.get('to')
    sensor_id=request.headers.get('sensor_id')

    sensor_id=((sensor_id.replace("[","")).replace("]","")).split(",")
    print(sensor_id[0])
    
    if sensor_id[0]!="":
        jsondata=list()
        conn = get_db_connection()
        for i in sensor_id:
            sensorid=int(i)
            posts = conn.execute("SELECT Company.ID,Company.company_name,Location.location_name,Location.location_country,Location.location_city,Location.location_meta,Sensor.sensor_id,Sensor.sensor_name,Sensor.sensor_category,Sensor.sensor_meta,Sensor_Data.medicion,Sensor_Data.fecha FROM Sensor_Data,Sensor,Location,Company where Company.company_api_key=? and Location.company_id=Company.ID and Sensor.sensor_id=? and Sensor.location_id=Location.location_id and Sensor_Data.api_key=Sensor.sensor_api_key and Sensor_Data.fecha between ? and ? ",(company,sensorid,desde,hasta)).fetchall()
            if posts.__len__()==0:
                None
            else:
                for i in posts:
                    jsondata.append({"ID_Company":i[0],"Company_name":i[1],"Location_name":i[2],"Location_country":i[3],"Location_city":i[4],"Location_meta":i[5],"Sensor_ID":i[6],"Sensor_name":i[7],"Sensor_category":i[8],"Sensor_meta":i[9],"Sensor_measurement":i[10],"Date_measurement":i[11]})
        conn.close() 
        if jsondata.__len__()==0:
            return jsonify({"Message":"Not Finded","status":204})
        else:
            return jsonify(jsondata,{"status":201})
    else:
        return jsonify({"Message":"Argument empty","status":204})

                
#########################################################################################################################################################################################



if __name__== "__main__":
    app.run(host='0.0.0.0', port=8080, debug = False, threaded = True)
