import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

mydb = mysql.connector.connect()

conn = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSERNAME'),
    password=os.getenv('PASSWORD'),
)

curr = conn.cursor()

def dataBaseInitialization(curr):

    curr = conn.cursor()

    curr.execute("""
    DROP DATABASE IF EXISTS musicDB;
    """)

    curr.execute("""
    CREATE DATABASE IF NOT EXISTS musicDB;
    """)

def useDB(curr):
    curr.execute('USE musicDB;')
    conn.commit()

def tableInitialization(curr):

    # 1. Country
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Country(
            country_id INT PRIMARY KEY AUTO_INCREMENT, 
            country VARCHAR(255) NOT NULL
        );
    """)

    # 2. Genre
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Genre(
            genre_id INT PRIMARY KEY, 
            genre VARCHAR(255) NOT NULL
        );
    """)

    # 3. Artist
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Artist(
            artist_id INT PRIMARY KEY AUTO_INCREMENT, 
            artist_name VARCHAR(255) NOT NULL UNIQUE
        );
    """)

    # 4. Album
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Album(
            album_id INT PRIMARY KEY, 
            title VARCHAR(255) NOT NULL
        );
    """)

    # 5. Customer
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Customer(
            customer_id INT PRIMARY KEY, 
            first_name VARCHAR(255) NOT NULL, 
            last_name VARCHAR(255) NOT NULL, 
            country_id INT NOT NULL, 
            FOREIGN KEY (country_id) REFERENCES Country(country_id)
        );
    """)

    # 6. Invoice
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Invoice(
            invoice_id INT PRIMARY KEY, 
            customer_id INT NOT NULL,
            invoice_date DATE NOT NULL, 
            total DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
        );
    """)

    # 7. Track
    curr.execute("""
        CREATE TABLE IF NOT EXISTS Track(
            track_id INT PRIMARY KEY,
            track_name VARCHAR(255) NOT NULL,
            album_id INT,
            genre_id INT,
            milliseconds INT,
            unit_price DECIMAL(10,2),
            FOREIGN KEY (album_id) REFERENCES Album(album_id),
            FOREIGN KEY (genre_id) REFERENCES Genre(genre_id)
        );
    """)

    # 8. InvoiceLine
    curr.execute("""
        CREATE TABLE IF NOT EXISTS InvoiceLine(
            invoice_line_id INT PRIMARY KEY,
            invoice_id INT,
            track_id INT,
            FOREIGN KEY (invoice_id) REFERENCES Invoice(invoice_id),
            FOREIGN KEY (track_id) REFERENCES Track(track_id)
        );
    """)

    # 9. AlbumArtist 
    curr.execute("""
        CREATE TABLE IF NOT EXISTS AlbumArtist(
            album_id INT,
            artist_id INT,
            PRIMARY KEY (album_id, artist_id),
            FOREIGN KEY (artist_id) REFERENCES Artist(artist_id),
            FOREIGN KEY (album_id) REFERENCES Album(album_id)
        );
    """)

    #Commit
    conn.commit()


def main(curr):
    dataBaseInitialization(curr)
    useDB(curr)
    tableInitialization(curr)

main(curr)