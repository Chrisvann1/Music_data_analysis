import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
from create_database import useDB
 
load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSERNAME'),
    password=os.getenv('PASSWORD'),
    #database="musicDB",   
)
 
curr = conn.cursor()

def queries(curr):

    #How much did each country generate in purchases?
    curr.execute("""
    SELECT co.country, ROUND(SUM(i.total), 2) AS revenue
    FROM Invoice i
    JOIN Customer c ON i.customer_id = c.customer_id
    JOIN Country co ON c.country_id = co.country_id
    GROUP BY co.country
    ORDER BY revenue DESC;
    """)

    query1 = curr.fetchall()

    df1 = pd.DataFrame(query1, columns=["country", "revenue"])
    df1["revenue"] = pd.to_numeric(df1["revenue"])
    print(df1)
    plt.barh(df1["country"], df1["revenue"], color="#455A2F")
    plt.title("How much did each country generate in purchases?")
    plt.xlabel("Revenue")
    plt.ylabel("Country")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.show()
    
    #How many times was each genre purchased in each country?
    curr.execute("""
    SELECT g.genre, COUNT(*) AS times_purchased
    FROM InvoiceLine il
    JOIN Track t ON il.track_id = t.track_id
    JOIN Genre g ON t.genre_id = g.genre_id
    JOIN Invoice i ON il.invoice_id = i.invoice_id
    JOIN Customer c ON i.customer_id = c.customer_id
    JOIN Country co ON co.country_id = c.country_id
    WHERE co.country = 'USA'
    GROUP BY g.genre, co.country
    ORDER BY times_purchased DESC;
    """)

    query2 = curr.fetchall()

    df2 = pd.DataFrame(query2, columns=["genre", "times_purchased"])

    plt.barh(df2["genre"], df2["times_purchased"], color="#455A2F")
    plt.title("Genre Purchases in the USA")
    plt.xlabel("Times Purchased")
    plt.ylabel("Genre")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.show()


    #10 most purchased Rock tracks
    curr.execute("""
    SELECT 
        t.track_name,
        COUNT(il.track_id) AS times_purchased
    FROM InvoiceLine il
    JOIN Track t ON il.track_id = t.track_id
    JOIN Genre g ON t.genre_id = g.genre_id
    WHERE g.genre = 'Rock'
    GROUP BY t.track_id, t.track_name
    ORDER BY times_purchased DESC
    LIMIT 10;
    """)

    query3 = curr.fetchall()
    df3 = pd.DataFrame(query3, columns=["track_name", "times_purchased"])
    print(df3)

    plt.barh(df3["track_name"], df3["times_purchased"], color="#455A2F")
    plt.xlabel("Times Purchased")
    plt.ylabel("Rock Track")
    plt.title("Most Purchased Rock Tracks")
    plt.tight_layout()
    plt.show()

def main(curr):
    useDB(curr)
    queries(curr)

main(curr)