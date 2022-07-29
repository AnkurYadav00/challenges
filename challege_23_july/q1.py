################ 1. create a table attribute dataset and dress dataset.#################

import mysql.connector as conn
import logging
import pandas as pd

logging.basicConfig(filename='mysqlq1.log', level=logging.INFO, format='%(levelname)s %(asctime)s %(message)s')

# setting connection with mmysql server.
mydb = conn.connect(host='localhost', user='root', password='Luv@2018')
cursor = mydb.cursor(buffered=True)

# checking connection with mysql
if mydb.is_connected():
    print("Database Connected")
else:
    print("Database not connected")


def Database_create(Challenge):
    try:
        cursor.execute(f"Create database {Challenge};")
        logging.info('DataBase Created')
        mydb.commit()
    except Exception as e:
        logging.exception(e)


Database_name = input('enter database name')

# calling function for database creation
Database_create(Database_name)

# Defining which database we will be using from now on.
cursor.execute(f"use {Database_name};")


def create_table_attribute():
    try:
        # attribute = pd.read_excel("Attribute DataSet.xlsx", header=None)
        cursor.execute(
            f"create table challenge.Attribute (Dress_ID int, Style varchar(30),Price varchar(30),Rating float,Size varchar(10),Season varchar(20),NeckLine varchar(20),SleeveLength varchar(30),waiseline varchar(30), Material varchar(30), FabricType varchar(30), Decoration varchar(30),PatternType varchar(30),Recommendation int)")
        mydb.commit()
        logging.info("Creating inside a database database")
    except Exception as e:
        logging.exception(e)


# calling create_tables to create attribute.
create_table_attribute()


def create_table_dress():
    try:
        logging.info("Creating dress table")
        cursor.execute(
            "create table challenge.dress (Dress_ID int,	`29/8/2013` int,	`31/8/2013` int,	`2/9/2013` int,	`4/9/2013` int,	`6/9/2013` int,	`8/9/2013` int,	`10/9/2013` int,	`12/9/2013` int,	`14/9/2013` int,	`16/9/2013` int,	`18/9/2013` int,	`20/9/2013` int,	`22/9/2013` int,	`24/9/2013` int,	`26/9/2013` int,	`28/9/2013` int,	`30/9/2013` int,	`2/10/2013` int,	`4/10/2013` int,	`6/10/2013` int,	`8/10/2010` int,	`10/10/2013` int,	`12/10/2013` int)")
        mydb.commit()
    except Exception as e:
        logging.info(e)


# calling dress table in database challenge.
create_table_dress()

