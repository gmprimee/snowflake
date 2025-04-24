#!/usr/bin/python3

import os
import socket
import snowflake.connector as sc
import re

# Variables for Snowflake connection
#
nldc1_naemon_log_file = "nldc1_naemon.log"
segum_naemon_log_file = "segum_naemon.log"
nldc1_notifications_log_file = "nldc1_notifications.log"
segum_notifications_log_file = "segum_notifications.log"
private_key_file='/home/gradmin/.ssh/snowflake_rsa_key.p8'
conn_params = {
    'account' : 'BL35257-VERISURE',
    'user' : 'grzegorz.martyniuk@verisure.com',
    'private_key_file' : private_key_file,
    'warehouse' : 'TEAM_INC_WH' ,
    'database' : 'TEAM_INC_DEV',
    'schema' : 'PUBLIC'
}
naemon_arr=[]
notifications_arr=[]

# Connect to DB and INSERT INTO notifications_log table 2D array data converted from naemon.log file
#
def db_insert(conn_params,private_key_file,site,dataset):
    print('Hello. Starting to connect to the snowflake...')
    try:
        con = sc.connect(**conn_params)
        cs = con.cursor()
        if len(dataset) < 10000 :
            for row in range(len(dataset)):
                command1=("INSERT INTO {3}_notifications_log  (date,time, data) VALUES ('{0}','{1}','{2}');".format(dataset[row][0],dataset[row][1],dataset[row][2],site))
                cs.execute(command1)
                print(row)
        else:
            for row in range(len(dataset)):
                command1=("INSERT INTO {5}_naemon_log (epoch_time,check_type, service, host, data) VALUES ('{0}','{1}','{2}','{3}','{4}');".format(dataset[row][0],dataset[row][1],dataset[row][2],dataset[row][3],dataset[row][4],site))
                cs.execute(command1)
                print(row)
    except BaseException as e:
        print ("There's been a problem with DB connection with following system message: \n", e)
    finally:
        con.close()

# Read and format naemon.log file
def naemon_log(logfile):
    del naemon_arr[:]
    naelog=open(logfile,'r')
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
def notifications_log(logfile):
    del notifications_arr[:]
    notlog=open(logfile,'r')
    for row in notlog:
        date = row.split(" ")[0]
        time = row.split(" ")[1]
        data = row.split(" ")[2:]
        data= " ".join(data)
        notifications_arr.append([date,time,data])
    return notifications_arr

if __name__ == "__main__":
    site=socket.gethostname().split("-")[0]
    print (site)
    site="nldc1"
    naemon_log(nldc1_naemon_log_file)
    notifications_log(nldc1_notifications_log_file)
    print(site,len(naemon_arr),len(notifications_arr))
    #db_insert(conn_params,private_key_file,site,notifications_arr)
    site="segum"
    naemon_log(segum_naemon_log_file)
    notifications_log(segum_notifications_log_file)
    print(site,len(naemon_arr),len(notifications_arr))
    #db_insert(conn_params,private_key_file,site,notifications_arr)

    #db_insert(conn_params,private_key_file,site,naemon_arr)
