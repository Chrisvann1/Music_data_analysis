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
    df1.plot(x="country", y="revenue", kind="bar")
    plt.xticks(rotation=45)
    plt.xlabel("Country")
    plt.ylabel("Revenue")
    plt.tight_layout()
    plt.show()
    
    #How many times was each genre purchased in each country?
    curr.execute("""
    SELECT g.genre, co.country, COUNT(*) AS times_purchased
    FROM InvoiceLine il
    JOIN Track t ON il.track_id = t.track_id
    JOIN Genre g ON t.genre_id = g.genre_id
    JOIN Invoice i ON il.invoice_id = i.invoice_id
    JOIN Customer c ON i.customer_id = c.customer_id
    JOIN Country co ON co.country_id = c.country_id
    GROUP BY g.genre, co.country
    ORDER BY times_purchased DESC;
    """)

    query2 = curr.fetchall()

    df2 = pd.DataFrame(query2, columns=["genre", "country", "times_purchased"])
    
    pivot_df2 = df2.pivot(
        index="genre",
        columns="country",
        values="times_purchased"
    ).fillna(0)
    
    pivot_df2.plot(kind="bar")
    
    plt.xticks(rotation=45)
    plt.xlabel("Genre")
    plt.ylabel("Times Purchased")
    plt.title("Genre Popularity by Country")
    
    plt.tight_layout()
    plt.show()

    #Which albums have the longest average songs and how many songs do they have?
    curr.execute("""
    SELECT a.title,
        ROUND(AVG(t.milliseconds) / 1000, 2) AS avg_track_length_seconds,
        COUNT(t.track_id) AS num_tracks
    FROM Album a
    JOIN Track t ON a.album_id = t.album_id
    GROUP BY a.album_id, a.title
    ORDER BY avg_track_length_seconds DESC;
    """)

    query3 = curr.fetchall()
    
    df3 = pd.DataFrame(query3, columns=["title", "avg_track_length_seconds", "num_tracks"])
    df3 = pd.DataFrame(query3, columns=["title", "avg_track_length_seconds", "num_tracks"])

    df3["num_tracks"] = pd.to_numeric(df3["num_tracks"])
    df3["avg_track_length_seconds"] = pd.to_numeric(df3["avg_track_length_seconds"])

    df3.plot(x='num_tracks', y='avg_track_length_seconds', kind='bar')
    plt.xlabel("Number of Tracks")
    plt.ylabel("Average Track Length (seconds)")
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

    query4 = curr.fetchall()
    df = pd.DataFrame(query4, columns=["track_name", "times_purchased"])
    print(df)

    df.plot(
    x="track_name",
    y="times_purchased",
    kind="bar"
    )

    plt.xticks(rotation=45)
    plt.xlabel("Rock Track")
    plt.ylabel("Times Purchased")
    plt.title("Most Purchased Rock Tracks")
    plt.tight_layout()
    plt.show()

def main(curr):
    useDB(curr)
    queries(curr)

main(curr)