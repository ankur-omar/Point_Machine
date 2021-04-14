#Database Operations


import psycopg2
import operator
from itertools import islice
print("Connecting to the Data base")
connection =psycopg2.connect(user ="postgres",
                                 password="Ankur@1998",host ="127.0.0.1",port="5432",database ="Point Machine Data")
cur=connection.cursor()

#FIND THE AVG OF ALL PARAMETERS WHERE THE OPERATION TYPE IS NORMAL
pc_avg =cur.execute("SELECT gearname,date, AVG(pc)::numeric(10,2) as pc_avg ,AVG(oc)::numeric(10,2) as oc_avg,AVG(ot)::numeric(10,2) as ot_avg,AVG(vns)::numeric(10,2) as vns_avg,AVG(vrs)::numeric(10,2) as vrs_avg FROM point_parameter where operationtype ='Normal Operation' and ot<6000   GROUP BY gearname,date ORDER BY gearname DESC LIMIT 10;")

pc_avg1 =cur.fetchall()
print(pc_avg1)
for i in range(len(pc_avg1)):
        operationtype ="Normal Operation"
        gear_name = pc_avg1[i][0]
        cur_date =pc_avg1[i][1]
        pc_avg2 = pc_avg1[i][2]
        oc_avg2 =pc_avg1[i][3]
        ot_avg2 = pc_avg1[i][4]
        vns_avg2 = pc_avg1[i][5]
        vrs_avg2 = pc_avg1[i][6]




        cur.execute("INSERT  INTO avg_data (pc_avg,oc_avg,ot_avg,vns_avg,vrs_avg,gearname,operationtype,today_date) values(%s,%s,%s,%s,%s,%s,%s,%s);", (pc_avg2, oc_avg2,ot_avg2,vns_avg2,vrs_avg2,gear_name,operationtype,cur_date))




# FIND THE AVG OF ALL PARAMETERS WHERE THE OPERATION TYPE IS NORMAL

pc_avg =cur.execute("SELECT gearname,date, AVG(pc)::numeric(10,2) as pc_avg ,AVG(oc)::numeric(10,2) as oc_avg,AVG(ot)::numeric(10,2) as ot_avg,AVG(vns)::numeric(10,2) as vns_avg,AVG(vrs)::numeric(10,2) as vrs_avg FROM point_parameter where operationtype ='Reverse Operation' and ot<6000 GROUP BY gearname,date ORDER BY gearname DESC LIMIT 10;")
pc_avg1 =cur.fetchall()
print(pc_avg1)
for i in range(len(pc_avg1)):
        operationtype ="Reverse Operation"
        gear_name = pc_avg1[i][0]
        cur_date =pc_avg1[i][1]
        pc_avg2 = pc_avg1[i][2]
        oc_avg2 =pc_avg1[i][3]
        ot_avg2 = pc_avg1[i][4]
        vns_avg2 = pc_avg1[i][5]
        vrs_avg2 = pc_avg1[i][6]



        cur.execute("INSERT  INTO avg_data (pc_avg,oc_avg,ot_avg,vns_avg,vrs_avg,gearname,operationtype,today_date) values(%s,%s,%s,%s,%s,%s,%s,%s);", (pc_avg2, oc_avg2,ot_avg2,vns_avg2,vrs_avg2,gear_name,operationtype,cur_date))


# fetch the latest value from point_parameter normal operation
cur.execute("select pc, oc,ot,gearname,operationtype from point_parameter ORDER BY id DESC LIMIT 1;")

latest_data_list =cur.fetchall()
for i in range(len(latest_data_list)):

   latest_pc_value =latest_data_list[i][0]
   latest_oc_value =latest_data_list[i][1]

   latest_ot_value =latest_data_list[i][2]
   latest_gearname_value =latest_data_list[i][3]
   latest_operationtype_value =latest_data_list[i][4]


avg_data_value = cur.execute("select pc_avg,oc_avg,ot_avg,gearname ,operationtype from avg_data where operationtype ='Normal Operation';")
avg_data_value = cur.fetchall()
print(avg_data_value)

for i in range(len(avg_data_value)):
        avg_pc_value = avg_data_value[i][0]
        avg_oc_value = avg_data_value[i][1]

        avg_ot_value = avg_data_value[i][2]

        avg_gear_value = avg_data_value[i][3]
        avg_operationtype_value = avg_data_value[i][4]

        if (avg_gear_value == latest_gearname_value and avg_operationtype_value == latest_operationtype_value):
            d_pc = ((latest_pc_value - avg_pc_value) / (avg_pc_value)) * 100

            d_pc = round(d_pc)
            print(d_pc)

            d_oc = ((latest_oc_value - avg_oc_value) / (avg_oc_value)) * 100

            d_oc = round(d_oc)
            print(d_oc)

            d_ot = ((latest_ot_value - avg_ot_value) / (avg_ot_value)) * 100

            d_ot = round(d_ot)


            print(d_ot)

            d_v = cur.execute(
                "SELECT gearname,operationtype,pc_error,oc_error,ot_error from deviation_data ORDER BY id DESC LIMIT 1;")
            d_v = cur.fetchall()
            for i in range(len(d_v)):
                d_v_gearname = d_v[i][0]
                d_v_operationtype = d_v[i][1]
                d_v_pc_error = d_v[i][2]
                d_v_oc_error = d_v[i][3]
                d_v_ot_error = d_v[i][4]

                if (
                        d_v_gearname == avg_gear_value and d_v_operationtype == avg_operationtype_value and d_v_pc_error == d_pc and d_v_oc_error == d_oc and d_v_ot_error == d_ot):
                    pass
                else:
                    cur.execute(
                        " INSERT INTO deviation_data (gearname,operationtype,pc_error,oc_error,ot_error) values(%s,%s,%s,%s,%s);",
                        (avg_gear_value, avg_operationtype_value, d_pc, d_oc, d_ot))


#reverse operation
cur.execute("select pc, oc,ot,gearname,operationtype from point_parameter ORDER BY id DESC LIMIT 1;")

latest_data_list = cur.fetchall()
for i in range(len(latest_data_list)):
    latest_pc_value = latest_data_list[i][0]
    latest_oc_value = latest_data_list[i][1]

    latest_ot_value = latest_data_list[i][2]
    latest_gearname_value = latest_data_list[i][3]
    latest_operationtype_value = latest_data_list[i][4]

avg_data_value = cur.execute(
    "select pc_avg,oc_avg,ot_avg,gearname ,operationtype from avg_data where operationtype ='Reverse Operation';")
avg_data_value = cur.fetchall()
print(avg_data_value)
for i in range(len(avg_data_value)):
    avg_pc_value = avg_data_value[i][0]
    avg_oc_value = avg_data_value[i][1]

    avg_ot_value = avg_data_value[i][2]

    avg_gear_value = avg_data_value[i][3]
    avg_operationtype_value = avg_data_value[i][4]

    if (avg_gear_value == latest_gearname_value and avg_operationtype_value == latest_operationtype_value):
        d_pc = ((latest_pc_value - avg_pc_value) / (avg_pc_value)) * 100

        d_pc = round(d_pc)
        print(d_pc)

        d_oc = ((latest_oc_value - avg_oc_value) / (avg_oc_value)) * 100

        d_oc = round(d_oc)
        print(d_oc)

        d_ot = ((latest_ot_value - avg_ot_value) / (avg_ot_value)) * 100

        d_ot = round(d_ot)
        print(d_ot)

        d_v =cur.execute("SELECT gearname,operationtype,pc_error,oc_error,ot_error from deviation_data ORDER BY id DESC LIMIT 1;")
        d_v =cur.fetchall()
        for i in range(len(d_v)):
            d_v_gearname =d_v[i][0]
            d_v_operationtype =d_v[i][1]
            d_v_pc_error =d_v[i][2]
            d_v_oc_error =d_v[i][3]
            d_v_ot_error =d_v[i][4]

            if(d_v_gearname ==avg_gear_value and d_v_operationtype==avg_operationtype_value and d_v_pc_error ==d_pc and d_v_oc_error==d_oc and d_v_ot_error ==d_ot ):
                pass
            else:
                cur.execute(
                    " INSERT INTO deviation_data (gearname,operationtype,pc_error,oc_error,ot_error) values(%s,%s,%s,%s,%s);",
                    (avg_gear_value, avg_operationtype_value, d_pc, d_oc, d_ot))

connection.commit()

cur.close()
connection.close()
#
#
