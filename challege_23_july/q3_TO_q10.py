###########################################################################################
#####################3.read these dataset in pandas as a dataframe.########################
import json

import pandas as pd
import logging
import mysql.connector as con
import pymongo

logging.basicConfig(filename="mysqlq3.log", level=logging.INFO, format="%(levelname)s %(asctime)s %(message)s")

mydb = con.connect(host='localhost', user='root', password='Luv@2018')
if mydb.is_connected():
    print("database connected")
    logging.info("database connected")
else:
    print("dataset disconnected")
    logging.error("database not connected")

# initializing cursor
cursor = mydb.cursor(buffered=True)
cursor.execute("use challenge;")  # using dataset challenge

# extracting attribute table data from mysql
df = pd.read_sql_query("select * from attribute;", con=mydb)
print(df)

# extracting attribute table data from mysql
df_dress = pd.read_sql_query("select * from dress", mydb)
print(tuple(df_dress))

#################################################################################
#################4.Convert attribute dataset in json format.#####################
# df.to_json("Attribute.json")
df_js = pd.read_json("Attribute.json")
print(df_js)

#################################################################################
#################5.Store this dataset into mongodb in a single go.###############
mon_db = pymongo.MongoClient(
    "mongodb+srv://USER:PASSWORD@cluster0.lc3s4.mongodb.net/?retryWrites=true&w=majority")
db = mon_db.test
print(db)

database = mon_db['challenge']  # creating database
collection = database['attribute']  # creating collection


def json_file_to_mongodb():
    try:
        with open('Attribute.json') as file:
            json_file = json.load(file)
        collection.insert_many([json_file])
    except Exception as e:
        logging.error(e)


json_file_to_mongodb()


################################################################################################################################
############6.In SQL task try to perform left join operation with attribute dataset and dress dataset on column dress_ID.#######
def left_join_attribute():
    try:
        cursor.execute("select * from attribute left join dress on attribute.Dress_ID = dress.Dress_ID;")
        logging.info("Query fired left_join_attribute")
    except Exception as e:
        logging.exception(e)


left_join_attribute()


#########################################################################################################################
####################7. Write a sql query o find out how many unique dress we have  based on dress id.####################
def unique_dress_count():
    try:
        cursor.execute("select count(distinct Dress_ID) as `unique_no of dress id` from attribute;")
        logging.info("query fired unique_dress_count")
    except Exception as e:
        logging.error(e)


unique_dress_count()

###################################################################################
###########8.Try to find out how many dress is having recommendation 0. ###########
def dress_No_recommendations():
    try:
        cursor.execute("select count(Dress_ID) from  attribute where Recommendation = 0;")
        logging.info("query fired dress_No_recommendations")
    except Exception as e:
        logging.exception(e)


dress_No_recommendations()


########################################################################################################################
##############################    9. Try to find out total sale for each dress_ID.######################################
cursor.execute("""Select Dress_ID, `29/8/2013`+
`31/8/2013`+
`2/9/2013`+
`4/9/2013`+
`6/9/2013`+
`8/9/2013`+
`10/9/2013`+
`12/9/2013`+
`14/9/2013`+
`16/9/2013`+
`18/9/2013`+
`20/9/2013`+
`22/9/2013`+
`24/9/2013`+
`26/9/2013`+
`28/9/2013`+
`30/9/2013`+
`2/10/2013`+
`4/10/2013`+
`6/10/2013`+
`8/10/2010`+
`10/10/2013`+
`12/10/2013`  as Total_sum from dress group by Dress_ID""")
mydb.commit()
for i in cursor.fetchall():
    print(i)


########################################################################################################################
##########################    10.Try to find out third highest most selling dress_ID.###################################
cursor.execute(f"""Select Dress_ID,min(Total_sale) from (Select Dress_ID, (`29/8/2013`+
`31/8/2013`+
`2/9/2013`+
`4/9/2013`+
`6/9/2013`+
`8/9/2013`+
`10/9/2013`+
`12/9/2013`+
`14/9/2013`+
`16/9/2013`+
`18/9/2013`+
`20/9/2013`+
`22/9/2013`+
`24/9/2013`+
`26/9/2013`+
`28/9/2013`+
`30/9/2013`+
`2/10/2013`+
`4/10/2013`+
`6/10/2013`+
`8/10/2010`+
`10/10/2013`+
`12/10/2013`)  as Total_sale from dress group by Dress_ID order by Total_sale DESC limit 3) as dress_table_01  """)
mydb.commit()
for i in cursor.fetchall():
    print(i)
