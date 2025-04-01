#!/usr/bin/python3

import os
import snowflake.connector as sc

# Variables for Snowflake connection
private_key_file='/home/gradmin/.ssh/snowflake_rsa_key.p8'
account='BL35257-VERISURE'
user='grzegorz.martyniuk@verisure.com'
warehouse='TEAM_INC_WH'
database='TEAM_INC_DEV'
schema='PUBLIC'


# Connect to DB
def db_connection(private_key_file,command):

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
        print(command)
        select1= cs.execute(" {} ",format(command))
        result = select1.fetchall()
        print (result)
    except:
        print ("There's been a problem with DB connection with following system message: \n",e)
    finally:
        con.close()


# Read and format naemon.log file
def naemon_log():
    nlog=open("naemon.log","r")
    for row in nlog:
        epoch = row.split("]")[0].replace("[","")
        state = row.split(" ")[1:4]
        if state[1].isupper() and state[2].isupper():
            print(state)


# Read and format notifications.log file
# def notifications_log():


# Read and format thruk.log file
# def thruk_log():


if __name__ == "__main__":
    #command="SELECT * FROM naemon_log;"
    #db_connection(private_key_file,command)
    naemon_log()
