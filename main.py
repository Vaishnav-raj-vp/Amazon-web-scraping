import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests
import psycopg2
all=[]
url ='https://www.amazon.in/gp/bestsellers/books'
server ="PostgreSQL 16"
database ="Amazon WS"
conn= psycopg2.connect(host="localhost",database=database,user="postgres",password="Vishnuqwerty@3097")
def get_data(url):
    response = requests.get(url)
    data = bs(response.content,'html.parser')
    return data

def transform(data):
    books = data.find_all('div',attrs={'class':'zg-grid-general-faceout'})

    for i in books:
        bookname = i.find('div','_cDEzb_p13n-sc-css-line-clamp-1_1Fn1y').text
        author = i.find_all('div','_cDEzb_p13n-sc-css-line-clamp-1_1Fn1y')
        price = i.find('span','p13n-sc-price')
        if price is not None:
            bookprice = price.text

        if len(author)<2:
            author = "unknown"
        else:
            author = author[1].text


        diva = i.find('div','a-icon-row')
        if diva:
            ratings=str(diva.find('a').text)
            customer_ratings = ratings[:ratings.index('stars')+5]
            customers_rated = ratings[ratings.index('stars')+5:]
            all1=[]

            if bookname:
                all1.append(bookname)
            else:
                all1.append('unknown book')
            if author:
                all1.append(author)
            else:
                all1.append('unknown')
            if customer_ratings:
                all1.append(customer_ratings)
            else:
                all1.append('-1')

            if customers_rated:
                all1.append(customers_rated)
            else:
                all1.append(0)

            all1.append(bookprice)
            all.append(all1)

    return all

def DataFramed(all):
    df=pd.DataFrame(all)
    df.columns=['Book_name','Author','Customer_ratings','Customers_rated','Book_price']
    df.index = range(1,len(df)+1)
    return df

def cleanse(df):
    df['Customer_ratings']=df['Customer_ratings'].apply(lambda x:x.split()[0]).astype(float)
    df['Customers_rated']= df['Customers_rated'].apply(lambda x:x.replace(',','')).astype(int)
    df['Book_price']=df['Book_price'].apply(lambda x:x.replace('₹','')).apply(lambda x:x.replace(',','.')).astype(float)


response=get_data(url)
trans=transform(response)
df=DataFramed(trans)
cleaned = cleanse(df)

cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Books_data (
        "Book_name" VARCHAR(255),
        "Author" VARCHAR(255),
        "Book_price" VARCHAR(255),
        "Customer_ratings" VARCHAR(255),
        "Customers_rated" VARCHAR(255)
    );
""")
conn.commit()
cursor.execute('select * FROM Books_data')
validate=cursor.fetchall()


for index,row in df.iterrows():

        cursor.execute("""insert into Books_data("Book_name","Author","Book_price","Customer_ratings","Customers_rated")
         values (%s,%s,%s,%s,%s)""",(row["Book_name"],row["Author"],
                                     row["Book_price"],row["Customer_ratings"],row["Customers_rated"])

                       )


conn.commit()

cursor.execute('select * FROM Books_data')
# print(cursor.fetchall())







