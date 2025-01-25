import re
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
from tabulate import tabulate


def ExtractData(filename):
    # Pattern of mail
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    # Date and time Pattern
    date_pattern = r'([A-Za-z]{3}\s?[A-Za-z]{3}\s{1,3}?\d{1,2} \d{2}:\d{2}:\d{2} \d{4})'
    # List to store data
    emailDateList = []
    messageids = []

    # Reading from file
    with open(filename, 'r') as file:
        for line in file:
            # Find email addresses in the line
            emails = re.findall(email_pattern, line)
            # Find email addresses in the line
            dates = re.findall(date_pattern, line)
            
            # Add the emails and dates found to the list
            if emails and dates and 'From' in line:
                emailDateList.append([emails, dates])

            if "Message-ID" in line:
                messageids.append(emails)
                
    for i in range(len(emailDateList)):
        a = " ".join(messageids[i])
        emailDateList[i].insert(0, a.split('@',1)[0])

    for i in emailDateList:
        print(i)
    
    return emailDateList

def TransformDateTime(DataList):
    formattedData = []

    for sub, sub2, sub3 in DataList:
        dt = (" ".join(sub3))
        email = (" ".join(sub2))
        
        date_obj = datetime.strptime(str(dt), "%a %b %d %H:%M:%S %Y")
        sub3 = date_obj.strftime("%Y-%m-%d %H:%M:%S")
        formattedData.append([sub, email, sub3])

    return formattedData

def Print2DData(DataList):
    for sub1 in DataList:
        print(sub1)

def MongodbConnection(uri):
    # uri = "mongodb+srv://jfrancis:Mongodb#123@cluster0.zsniu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. Successfully connected to MongoDB!")
        return True
    except Exception as e:
        print(e)
        return False
    
def LoadingData(uri,collectionName, dataToLoad):
    try:
        if MongodbConnection(uri) == False:
            return
        
        client = MongoClient(uri)
        db = client.GuviProjectDatabase    # Create new Db called GuviProjectDatabase
        # collection = db.User_History  # Creating new Collection in the DB
        
        if(collectionName not in db.list_collection_names()):
            collection = db.collectionName
        else:
            print(f"{collectionName} already exist")
            return collection
            
       

        for sub, sub2, sub3 in dataToLoad:
            collection.insert_one({"_id": sub, "EmailAddress": sub2, "SentDate": sub3})

        print("All extracted data loaded to Mongodb")
        return collection
    except Exception as e:
        print(e)

def ReadDataFromMongodb(collection):
    documents = collection.find()

    for i in documents:
        print(i)

def SQLConnection():
    connection = mysql.connector.connect(
    host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    port = 4000,
    user = "3GNYKqCbbJtp9c2.root",
    password = "XQYbmm6hIa6n0Hwg",
    database = "FirstDatabase",
    )

    if connection.is_connected():
        print("Connection successful")
    else:
        print("Connection Failed")

    return connection

def UploadDataToSQL(dbname, collection):
    mycursor = SQLConnection().cursor()
    mycursor.execute("use GuviProjectDB")

    try:

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {dbname} (
            Message_id VARCHAR(50) PRIMARY KEY,
            Email VARCHAR(60),
            SentDate DATETIME)"""

        mycursor.execute(create_table_query)

        documents = collection.find()
        i = 0
        for d in documents:
            msg_id = d.get('_id')
            email = d.get('EmailAddress')
            sentdate = d.get('SentDate')

            insertQuery = """ 
                            Insert into GuviProjectDB.my_table_2 (Message_id, Email, SentDate)
                            values (%s, %s, %s)"""
            values = (msg_id, email, sentdate)

            mycursor.execute(insertQuery, values)
            print(i, f"{email} record inserted")
            i +=1
            SQLConnection().commit()
    except Exception as e:
        print(e)

def SampleQuery1():
    mycursor = SQLConnection().cursor()

    mycursor.execute("select distinct Email from GuviProjectDB.my_table")
    out = mycursor.fetchall()
    print(tabulate(out, headers=[i[0] for i in mycursor.description], showindex="always", tablefmt='psql'))

def SampleQuery2():
    mycursor = SQLConnection().cursor()

    mycursor.execute("""
            SELECT 
                DATE(SentDate) AS email_date,
                COUNT(*) AS emails_received
            FROM 
                GuviProjectDB.my_table
            GROUP BY 
                email_date
            ORDER BY 
                emails_received desc;""")
                 
    out = mycursor.fetchall()
    print(tabulate(out, headers=[i[0] for i in mycursor.description], showindex="always", tablefmt='psql'))

def SampleQuery3():
    mycursor = SQLConnection().cursor()

    mycursor.execute("""
            SELECT 
                Email,
                MIN(SentDate) AS first_email_date,
                MAX(SentDate) AS last_email_date
            FROM 
                GuviProjectDB.my_table
            GROUP BY 
                Email;""")
    out = mycursor.fetchall()
    print(tabulate(out, headers=[i[0] for i in mycursor.description], showindex="always", tablefmt='psql'))


def SampleQuery4():
    mycursor = SQLConnection().cursor()
    mycursor.execute("""
            SELECT 
                SUBSTRING_INDEX(Email, '@', -1) AS Domain_Names,
                COUNT(*) AS total_emails
            FROM 
                GuviProjectDB.my_table
            GROUP BY 
                Domain_Names
            ORDER BY 
                total_emails DESC;""")

    out = mycursor.fetchall()
    print(tabulate(out, headers=[i[0] for i in mycursor.description], showindex="always", tablefmt='psql'))

def AllQueries():
    SampleQuery1()
    SampleQuery2()
    SampleQuery3()
    SampleQuery4()



dbName = 'User_History'
file = r'GuviProjects\Project_1_ServerLogFile\mbox.txt'
extractedData = ExtractData(file)
# print(extractedData)
transformedData = TransformDateTime(extractedData)
uri = "mongodb+srv://jfrancis:Mongodb#123@cluster0.zsniu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
loadedData = LoadingData(uri, dbName, transformedData)
UploadDataToSQL(dbName, loadedData)
AllQueries()
