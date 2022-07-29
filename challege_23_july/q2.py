##################### 2. do a bulk load for these two tables for respective dataset.##################
import mysql.connector as con
import logging
import pandas as pd
import numpy as np

logging.basicConfig(filename="mysqlq2.log", level=logging.INFO, format="%(levelname)s %(asctime)s %(message)s")

mydb = con.connect(host='localhost', user='root', password='Luv@2018')
cursor = mydb.cursor(buffered=True)

if mydb.is_connected():
    logging.info("database connected")
    print("database connected")

else:
    logging.info("database not connected")
    print("database not connected")

# converting separator in attribute excel
df_attribute = pd.read_excel(
    r"C:\Users\Ankur  Yadav\Documents\jupyter\Untitled Folder 1\FD_DATASET\Attribute DataSet.xlsx", engine='openpyxl')
df_dress = pd.read_excel(r"C:\Users\Ankur  Yadav\Documents\jupyter\Untitled Folder 1\FD_DATASET\Dress Sales.xlsx",
                         engine='openpyxl')


# first method to insert data inside table.
def attribute_insert_data():
    try:
        cursor.execute("use challenge")
        logging.info("using challenge database")
        cursor.execute(f""" insert into attribute values(1006032852,'sexy','Low',4.6,'M','Summer','o-neck','sleevless','empire','','chiffon','ruffles','animal',1),
        (1212192089,'Casual','Low',0.0,'L','Summer','o-neck','Petal','natural','microfiber','','ruffles','animal',0),
        (1190380701,'Vintage','High',0.0,'L','Automn','o-neck','full','natural','polyster','','','print',0),
        (966005983,'Brief','Average',4.6,'L','Spring','o-neck','full','natural','silk','chiffon','embroidary','print',1),
        (876339541,'cute','Low',4.5,'M','Summer','o-neck','butterfly','natural','chiffonfabric','chiffon','bow','dot',0),
        (1068332458,'bohemian','Low',0.0,'M','Summer','v-neck','sleevless','empire','','','','print',0),
        (1220707172,'Casual','Average',0.0,'XL','Summer','o-neck','full','','cotton','','','solid',0),
        (1219677488,'Novelty','Average',0.0,'free','Automn','o-neck','short','natural','polyster','broadcloth','lace','',0),
        (1113094204,'Flare','Average',0.0,'free','Spring','v-neck','short','empire','cotton','broadcloth','beading','solid',1),
        (985292672,'bohemian','Low',0.0,'free','Summer','v-neck','sleevless','natural','nylon','chiffon','','',1),
        (1117293701,'party','Average',5.0,'free','Summer','o-neck','full','natural','polyster','broadcloth','lace','solid',0),
        (898481530,'Flare','Average',0.0,'free','Spring','v-neck','short','','nylon','','','animal',0),
        (957723897,'sexy','Low',4.7,'M','Winter','o-neck','threequarter','','','chiffon','lace','print',1),
        (749031896,'Vintage','Average',4.8,'M','Summer','o-neck','short','empire','cotton','jersey','','animal',1),
        (1055411544,'Casual','Low',5.0,'M','Summer','boat-neck','short','','cotton','','sashes','solid',0),
        (1162628131,'Casual','Low',0.0,'free','Winter','boat-neck','full','','other','other','lace','',0),
        (624314841,'cute','Average',4.7,'L','spring','o-neck','short','','cotton','','sashes','solid',1),
        (830467746,'bohemian','Medium',5.0,'free','Automn','o-neck','full','natural','','','hol Low out','patchwork',1),
        (840857118,'Brief','Average',0.0,'M','Winter','peterpan-collor','threequarter','natural','cotton','','','patchwork',0),
        (1113221101,'sexy','Average',5.0,'M','Automn','o-neck','sleevless','empire','milksilk','','','',1),
        (861754372,'sexy','Average',4.5,'L','Automn','o-neck','full','','cotton','','beading','solid',0),
        (856178100,'Casual','Low',4.3,'M','Summer','o-neck','sleevless','natural','','chiffon','','solid',0),
        (1122989777,'Brief','Low',4.0,'XL','Summer','v-neck','short','natural','cotton','','pockets','solid',0),
        (840516484,'sexy','Average',4.7,'S','Summer','v-neck','sleevless','empire','cotton','','sequined','solid',1),
        (768517084,'sexy','Average',0.0,'free','Automn','v-neck','sleevless','natural','polyster','','lace','patchwork',0),
        (1139843344,'sexy','Average',0.0,'M','Automn','o-neck','sleevless','empire','cotton','broadcloth','','print',0),
        (1004212992,'sexy','Average',4.7,'M','Spring','o-neck','sleevless','natural','','','','solid',1),
        (1235426503,'Casual','Low',0.0,'L','Summer','o-neck','sleevless','natural','cotton','jersey','','print',0),
        (942808364,'cute','Low',4.3,'free','Automn','o-neck','sleevless','natural','polyster','chiffon','sashes','striped',0),
        (629131530,'cute','Low',4.7,'M','Spring','ruffled','short','empire','chiffonfabric','chiffon','bow','dot',1),
        (851945460,'Casual','Average',4.6,'L','Automn','o-neck','sleevless','empire','polyster','chiffon','ruffles','solid',0),
        (1150275464,'Casual','Low',0.0,'M','Spring','o-neck','sleevless','natural','silk','chiffon','applique','solid',1),
        (1026634314,'Casual','Average',4.4,'L','Automn','o-neck','sleevless','natural','linen','chiffon','ruffles','animal',0),
        (978773911,'Brief','Average',4.3,'L','Spring','o-neck','sleevless','empire','','','bow','solid',1) """)
        logging.info("Data Inserted")
        mydb.commit("inserted  values Committed to database")
    except Exception as e:
        logging.error(e)


# 2nd method to insert data:-
# ----------------------------------------------------------------------------------------------- #
def insert_data_inTable(dataset_name, table, data: tuple):
    try:
        cursor.execute(f"insert into {dataset_name}.{table} values{data}")
        logging.info("Data inserted")
    except Exception as e:
        logging.error(e)


# inserting data in attribute table
# df_attribute.replace( np.nan, '', inplace=True)
for i in range(1, len(df_attribute)):
    insert_data_inTable('challenge', 'attribute', tuple(df_attribute.loc[i]))
    mydb.commit()

# inserting data in dress table
for i in range(len(df_dress)):
    insert_data_inTable('challenge', 'dress', tuple(df_dress.loc[i]))
    mydb.commit()

# ------------------------------------------------------------------------------------------------- #
