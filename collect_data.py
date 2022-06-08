import pymysql
import pandas as pd
import requests
import os

class Config:
  MYSQL_HOST = os.getenv("MYSQL_HOST")
  MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
  MYSQL_USER = os.getenv("MYSQL_USER")
  MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
  MYSQL_DB = os.getenv("MYSQL_DB")
  MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")


# Connect to the database
connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)

#[{'Tables_in_r2de2': 'audible_data'}, {'Tables_in_r2de2': 'audible_transaction'}]

#use connection.cursor() 
with connection.cursor() as cursor:
  cursor.execute("SELECT * FROM audible_data;")
  result = cursor.fetchall()

audible_data = pd.DataFrame(result)
#set column Book_Id to index 
audible_data = audible_data.set_index("Book_ID")

#use read_sql()
sql = "SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)

#merge table
transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

#import Currency conversion API
url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
r = requests.get(url)
result_conversion_rate = r.json()

#convert to dataframe
conversion_rate = pd.DataFrame(result_conversion_rate)

#set index = colum date
conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})

# ก็อปปี้ column timestamp เก็บเอาไว้ใน column ใหม่ชื่อ date เพื่อที่จะแปลงวันที่เป็น date เพื่อที่จะสามารถนำมา join กับข้อมูลค่าเงินได้
transaction['date'] = transaction['timestamp']

# แปลงให้จาก timestamp เป็น date ในทั้ง 2 dataframe (transaction, conversion_rate)
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

# รวม 2 dataframe (transaction, conversion_rate) เข้าด้วยกันด้วยคำสั่ง merge
# ผลลัพธ์สุดท้ายตั้งชื่อว่า final_df
final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")

# แปลงราคา โดยเอาเครื่องหมาย $ ออก และแปลงให้เป็น float
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)

#เพิ่ม column 'THBPrice' ที่เกิดจาก column Price * conversion_rate
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
final_df = final_df.drop(["date", "book_id"], axis=1)
final_df = final_df.drop("date", axis=1)

# save "to csv" file
final_df.to_csv("data_output.csv", index=False)
