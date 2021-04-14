# In this python file I have Subscribe the Point Machine Data with the help of python Library that is Paho Mqtt
# and store the data in postgresql database for this challenge i have used python psycopg2 library that is  helpful to work with postgresql
# using python.

import paho.mqtt.subscribe as subscribe
import psycopg2
import json
from datetime import datetime
print("Connecting to the Data base")
conn =psycopg2.connect(user ="postgres", password="Ankur@1998",host ="127.0.0.1",port="5432",dbname ="Point Machine Data")

def on_message_print(client, userdata, message):
    s=message.payload
    #s2 =b'{"STN":"FTP","VER":"5.6","LOC":"LOC04","DATE":"23-02-21","TIME":"17-44-43","TEMP":"26","RH":"32","RSSI":"65","NAME":"P310J","GEARTYPE":"PM","SUBGEAR":"A","EVENTTYPE":"4","VNS":"397543","VRS":"59819","VRRS":"0","VNO":"399967","VRO":"57152","VRRO":"0","EVENTTIME":[0,14,24,35,45,55,65,75,85,96,106,117,127,140,150,161,171,181,191,201,211,222,232,243,253,266,277,287,298,308,318,329,339,350,360,370,380,392,402,413,423,433,443,453,463,473,483,493,503,516,527,537,548,558,568,579,591,602,612,623,634,647,657,667,677,688,698,709,719,729,739,749,759,773,783,793,803,814,824,835,845,855,865,875,885,899,909,941,951,961,971,982,992,1002,1012,1022,1032,1045,1055,1066,1077,1177,1277,1377,1477,1577,1677,1777,1877,1977,2077,2177,2277,2377,2477,2577,2677,2777,2877,2977,3078,3179,3279,3379,3479,3580,3680,3781,3881,3981,4082,4183,4283,4383,4483,4583,4683,4783,4884,4985,5086,5186,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"EVENTDATA1":[2616,2679,2723,2763,2802,2829,2870,2604,2469,2281,2160,2137,2111,2089,2060,2084,2079,2075,2073,2076,2081,2086,2089,2097,2105,2113,2119,2123,2130,2134,2138,2144,2148,2153,2179,2163,2174,2184,2189,2189,2188,2189,2187,2187,2185,2182,2183,2180,2182,2181,2179,2179,2178,2180,2176,2177,2173,2174,2173,2169,2170,2169,2170,2168,2170,2170,2173,2175,2173,2170,2173,2173,2173,2169,2172,2170,2172,2170,2171,2173,2171,2170,2173,2172,2171,2170,2168,2170,2166,2167,2169,2170,2169,2167,2164,2166,2163,2165,2165,2163,2166,2163,2163,2162,2160,2154,2155,2157,2157,2156,2153,2148,2147,2149,2150,2147,2142,2138,2137,2137,2135,2136,2138,2139,2133,2132,2132,2130,2131,2131,2128,2129,2134,2145,2146,2140,2138,2142,2150,2144,2021,1384,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}'
    s2 = s.replace(b'\\', b'\\\\') # Handle the situation for "//"
    s2 =s2.replace(b',,',b',')
    #print(s2)




    message_dict = json.loads(s2)
    #print(message_dict)# parse the json string in to python object(Dictionary)
    #print(message_dict)
    if "STN" not in message_dict:
        return
    station_code = message_dict["STN"]
    if "DATE" not in message_dict:
        return
    if message_dict["DATE"] =="00-00-00":
        return
    date_str = message_dict["DATE"]
    #print(date_str)
    date_time_obj = datetime.strptime(date_str, '%d-%m-%y')
    #print("date time obj", date_time_obj)
    if "VER" not in message_dict:
        return
    ver_dict = message_dict["VER"]

    loc_dict = message_dict["LOC"]

    time_str = message_dict["TIME"]
    time_obj = datetime.strptime(time_str, '%H-%M-%S')

    rh_dict = message_dict["RH"]

    rssi_dict = message_dict["RSSI"]

    name_dict = message_dict["NAME"]
    if "SUBGEAR" not in message_dict:
        return
    subgear_dict = message_dict["SUBGEAR"]

    if "EVENTTYPE" not in message_dict:
        return
    eventtype_dict = message_dict["EVENTTYPE"]

    if "VNS" not in message_dict:
        return
    vns_dict = message_dict["VNS"]

    vrs_dict = message_dict["VRS"]

    vrrs_dict = message_dict["VRRS"]

    vno_dict = message_dict["VNO"]

    vro_dict = message_dict["VRO"]

    vrro_dict = message_dict["VRRO"]

    eventtime_list = message_dict["EVENTTIME"]

    eventdata1_list = message_dict["EVENTDATA1"]

    temperature = None
    if "TEMP" in message_dict:
        temperature = int(message_dict["TEMP"])
    # filter the data- topic- sensor_data5/FTP/PM
    if station_code != "FTP":
        return
    if "GEARTYPE" not in message_dict:
        return
    gear_type = message_dict["GEARTYPE"]
    if gear_type != "PM":
        return
    # if name_dict !="297A":
    #     return

    peak_current = 0
    operating_current = 0
    for i in range(len(eventdata1_list)):
        if eventtime_list[i] < 1000:
            peak_current = max(eventdata1_list[i], peak_current)
    print("Peak Current", peak_current)

    for i in range(len(eventdata1_list)):
        if eventtime_list[i] > 1000:
            operating_current = max(eventdata1_list[i], operating_current)
    print("Operating current", operating_current)

    operating_time = max(eventtime_list)
    print("Max operating time", operating_time)

    if vno_dict>vro_dict:
         no ="Normal Operation"
    elif vro_dict>vno_dict:
        no ="Reverse Operation"
    else:
        no ="Undefined"

    cur = conn.cursor()
    # SQL Statement for insert the data in to postgresql database

    cur.execute(
        "INSERT INTO pointdata2 (stn, date, temp,ver,loc,time,rh,rssi,name,geartype,subgear,eventtype,vns,vrs,vrrs,vno,vro,vrro,eventtime,eventdata1,pc,oc,ot,operationtype) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
        (station_code, date_time_obj, temperature,
         ver_dict, loc_dict, time_obj, rh_dict, rssi_dict, name_dict, gear_type, subgear_dict
         , eventtype_dict, vns_dict, vrs_dict, vrrs_dict, vno_dict, vro_dict, vrro_dict, eventtime_list,
         eventdata1_list, peak_current, operating_current, operating_time,no))
    cur.execute("INSERT INTO point_parameter(pc,oc,ot,vns,vrs,gearname,operationtype,date,time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s);",(peak_current,operating_current,operating_time,vns_dict,vrs_dict,name_dict,no,date_time_obj,time_obj))
    # Commit the database
    conn.commit()


subscribe.callback(on_message_print, "sensor_data5", hostname="148.72.206.253")

#conn.commit()
conn.close()








