import mysql.connector

def get_user_db(user):
    mydb = mysql.connector.connect(host='35.222.151.28', user='root', password='budget-tracker', database=user)
    return mydb