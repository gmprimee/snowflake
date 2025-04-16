#!/usr/bin/python3

import os
import snowflake.connector as sc
import re

# Variables for Snowflake connection
private_key_file='/home/gradmin/.ssh/snowflake_rsa_key.p8'
account='BL35257-VERISURE'
user='grzegorz.martyniuk@verisure.com'
warehouse='TEAM_INC_WH'
database='TEAM_INC_DEV'
schema='PUBLIC'

naemon_arr=[]
notifications_arr=[]

# Connect to DB and INSERT INTO naemon_log table 2D array data converted from naemon.log file
#
def naemon_insert(private_key_file,dataset):

    print('Hello. Starting to connect to the snowflake...')
    try:
        conn_params = {
            'account' : 'BL35257-VERISURE',
            'user' : 'grzegorz.martyniuk@verisure.com',
            'private_key_file' : private_key_file,
            'warehouse' : 'TEAM_INC_WH' ,
            'database' : 'TEAM_INC_DEV',
            'schema' : 'PUBLIC'
        }
        con = sc.connect(**conn_params)
        cs = con.cursor()
        for row in range(len(dataset)):
            command=("INSERT INTO NAEMON_LOG (epoch_time,check_type, service, host, data) VALUES ('{0}','{1}','{2}','{3}', '{4}');".format(dataset[row][0],dataset[row][1],dataset[row][2],dataset[row][3],dataset[row][4]))
            cs.execute(command)
            print(row)
        #result = select1.fetchall()
    except BaseException as e:
        print ("There's been a problem with DB connection with following system message: \n", e)
    finally:
        con.close()

# Connect to DB and INSERT INTO notifications_log table 2D array data converted from naemon.log file
#
def notification_insert(private_key_file,dataset):

    print('Hello. Starting to connect to the snowflake...')
    try:
        conn_params = {
            'account' : 'BL35257-VERISURE',
            'user' : 'grzegorz.martyniuk@verisure.com',
            'private_key_file' : private_key_file,
            'warehouse' : 'TEAM_INC_WH' ,
            'database' : 'TEAM_INC_DEV',
            'schema' : 'PUBLIC'
        }
        con = sc.connect(**conn_params)
        cs = con.cursor()
        for row in range(len(dataset)):
            command=("INSERT INTO NOTIFICATIONS_LOG  (date,time, data) VALUES ('{0}','{1}','{2}');".format(dataset[row][0],dataset[row][1],dataset[row][2]))
            cs.execute(command)
            print(row)
        #result = select1.fetchall()
    except BaseException as e:
        print ("There's been a problem with DB connection with following system message: \n", e)
    finally:
        con.close()

# Read and format naemon.log file
def naemon_log():

    naelog=open("naemon.log","r")
    for row in naelog:
        epoch = row.split("]")[0].replace("[","")
        status = row.split(":")[0].split(" ")[1:]
        data_line=row.split(":")[1:]
        if status[0].isupper() and status[1].isupper():
            if  status[0] == 'HOST' or status[1] == 'HOST':
                data=":".join(data_line).split(";")
                host=data[0]
                service="Host check"
                data=";".join(data[1:])
            #    print(epoch," ".join(status),host, service, data)
                naemon_arr.append([epoch," ".join(status),host,service,data])
            elif status[0] == 'SERVICE' or status[1] == 'SERVICE':
                data=":".join(data_line).split(";")
                host=data[0]
                service=data[1]
                data=";".join(data[2:])
            #    print(epoch," ".join(status), host, service, data)
                naemon_arr.append([epoch," ".join(status),host,service,data])
            else:
                data=" ".join(data_line)
                host="n/a"
                service="n/a"
            #    print (epoch," ".join(status), data)
                naemon_arr.append([epoch," ".join(status),host,service,data])
        elif status[0] == "Warning":
            if "of host" in data_line[0].split("'")[0]:
                host=data_line[0].split("'")[1]
                service="Host check"
                data=" ".join(data_line)
            #    print (epoch," ".join(status),host, service, data )
                naemon_arr.append([epoch," ".join(status),host,service,data])
            elif "of service"  in  data_line[0].split("'")[0] and not "External" in data_line[0].split("'")[0] or "Service" in  data_line[0].split("'")[0]:
                host=" ".join(data_line).split("'")[3]
                service=data_line[0].split("'")[1]
                data=" ".join(data_line)
             #   print (epoch," ".join(status), host, service, data)
                naemon_arr.append([epoch," ".join(status),host,service,data])
            else:
                data=" ".join(data_line)
                host="n/a"
                service="n/a"
             #   print(epoch," ".join(status),host,data)
                naemon_arr.append([epoch," ".join(status),host,service,data])
        else:
            data=" ".join(row.split("]"))[1:]
            status="System message"
            host="n/a"
            service="n/a"
            #print(epoch,status , host, service, data)
            naemon_arr.append([epoch,status,host,service,data])
    return naemon_arr

# Read and format notifications.log file
def notifications_log():
    notlog=open('notifications.log','r')
    for row in notlog:
        date = row.split(" ")[0]
        time = row.split(" ")[1]
        data = row.split(" ")[2:]
        data= " ".join(data)
        notifications_arr.append([date,time,data])
    print(notifications_arr)

if __name__ == "__main__":
    #naemon_log()
    #db_connection(private_key_file,naemon_arr)
    notifications_log()
    notification_insert(private_key_file,notifications_arr)
