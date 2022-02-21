import mysql.connector

def get_password():
    with open('db_password.txt', 'r') as f:
        return f.read()

def get_user_db(user):
    mydb = mysql.connector.connect(host='35.222.151.28', user='root', password=get_password(), database=user)
    return mydb