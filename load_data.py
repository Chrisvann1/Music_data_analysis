import mysql.connector
from dotenv import load_dotenv
import csv
import os
from create_database import useDB

load_dotenv()

mydb = mysql.connector.connect()

conn = mysql.connector.connect(
    host=os.getenv('DBHOST'),
    user=os.getenv('DBUSERNAME'),
    password=os.getenv('PASSWORD'),
)

curr = conn.cursor()

def loadCountry(curr):
    with open("dataset/customer.csv", "r") as file: 
        reader = csv.DictReader(file)

        for row in reader: 
            #Check to see if it is already in the table 
            curr.execute(
                f"SELECT country FROM Country WHERE country = '{row['country']}'"
            )

            country = curr.fetchone()

            #INSERT country into database if not there
            if country is None: 
                curr.execute(f""" 
                    INSERT INTO Country (country) 
                            VALUES ('{row['country']}')
                """)
        conn.commit()


def loadCustomer(curr):
    with open ("dataset/customer.csv") as file:
        reader = csv.DictReader(file)
        for row in reader: 
            #Check to see the corresponding country to create primary/foreign key relationship
            curr.execute(
                f"SELECT country_id FROM Country WHERE country = '{row['country']}'"
            )

            #Insert into customer table
            country_id = curr.fetchone()[0]
            curr.execute(f"""
                INSERT INTO Customer (customer_id,first_name,last_name,country_id)
                VALUES ('{row['customer_id']}','{row['first_name']}','{row['last_name'].replace("'","''")}',{country_id})
            """)


        conn.commit()

#Used double with 2 decimals to the right to get rid of repeated 0s after second decimal
def loadInvoice(curr): 
    with open ("dataset/invoice.csv") as file: 
        reader = csv.DictReader(file)
        for row in reader:  

            curr.execute(f"""
                INSERT INTO Invoice (invoice_id,customer_id,invoice_date,total)
                VALUES ('{row['invoice_id']}','{row['customer_id']}','{row['invoice_date']}','{row['total']}')
            """)

        conn.commit()


#There are rows that have the same invoice_id as track_id, not 
#a valid primary key for this data
#This means the quantity column is pointless because its logging each 
# invoice_id/track_id separately rather than combining them through the quantity column
def loadInvoiceLine(curr):
    with open ("dataset/invoice_line.csv") as file: 
        reader = csv.DictReader(file)
        for row in reader:  

            curr.execute(f"""
                INSERT INTO InvoiceLine (invoice_line_id,invoice_id,track_id)
                VALUES ('{row['invoice_line_id']}','{row['invoice_id']}','{row['track_id']}')
            """)

        conn.commit()

def loadTrack(curr):
    with open ("dataset/track.csv") as file: 
        reader = csv.DictReader(file)
        for row in reader:  

            curr.execute(f"""
                INSERT INTO Track (track_id,track_name,album_id,genre_id,milliseconds,unit_price)
                VALUES ('{row['track_id']}','{row['name'].replace("'","''")}','{row['album_id']}','{row['genre_id']}','{row['milliseconds']}','{row['unit_price']}')
            """)

        conn.commit()

def loadGenre(curr):
    with open ("dataset/genre.csv") as file: 
        reader = csv.DictReader(file)
        for row in reader:  

            curr.execute(f"""
                INSERT INTO Genre (genre_id, genre)
                VALUES ('{row['genre_id']}','{row['name']}')
            """)

        conn.commit()

#Album title replace to account for quotation problem
def loadAlbum(curr):
    with open ("dataset/album.csv") as file: 
        reader = csv.DictReader(file)
        for row in reader:  

            curr.execute(f"""
                INSERT INTO Album (album_id, title)
                VALUES ('{row['album_id']}','{row['title'].replace("'","''")}')
            """)

        conn.commit()
        
# Create a dict of the cleaned_data mapping – should be saved in main and passed as a parameter later
def artistDict(): 
    artist_dict = {}
    with open("dataset/artist_cleaned.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)

        #get artist_id and cleaned row
        for row in reader:
            artist_id = row[0]     
            clean_name = row[2]   

            artists = []
            parts = clean_name.split('|')

            for part in parts:
                part = part.strip()
                if part != "":
                    artists.append(part)

            artist_dict[artist_id] = artists

    return artist_dict

"""
We are going to assume uniqueness for the purposes of this pipeline 
or else I genuinely have no clue how else we would handle the data 
cleaning here and the AlbumArtist table 
"""
def loadArtist(curr, artist_dict):
    for artists in artist_dict.values():
        for name in artists:

            #Check if artists is in database (looping through each list in values of dictionary)
            curr.execute(f"""
                SELECT artist_id
                FROM Artist
                WHERE artist_name = '{name.replace("'","''")}'
            """)

            result = curr.fetchone()

           #If not, add artist name to database
            if result is None:
                curr.execute(f"""
                    INSERT INTO Artist (artist_name)
                    VALUES ('{name.replace("'","''")}')
                """)
    
    conn.commit()

"""
- Look at ID in album table 
- Find this key in Dict and the corresponding artists that exist in the list of values 
- For each artists we will be using SELECT to find the ID of that artist inside of the artist table 
    - Create row for each artist_id in list inside dict
"""
def loadAlbumArtist(curr, artist_dict):
    with open("dataset/album.csv", "r") as file:
        reader = csv.DictReader(file)

        #Check each row in album csv
        for row in reader:
            previous_artist_id = row['artist_id']

            #Error Check
            if not previous_artist_id in artist_dict:
                print("Error in the data pipeline")
                return
            
            #Get all of the artists corresponding to the 
            #original cleaned_data artist_id
            artists = artist_dict[previous_artist_id]

            #Create a row for each artist corresponding to this ID after 
            #the split
            for name in artists:
                #Get the current ID of the artist in the artist_table 
                #(assuming uniqueness)
                curr.execute(f"""
                    SELECT artist_id
                    FROM Artist
                    WHERE artist_name = '{name.replace("'","''")}'
                """)

                result = curr.fetchone()
                if result is None: 
                    print("Error in the pipeline")
                    return
                
                artist_id = result[0]
                album_id = row["album_id"]

                # Insert into AlbumArtist table
                curr.execute(f"""
                    INSERT INTO AlbumArtist (album_id, artist_id)
                    VALUES ({album_id}, {artist_id})
                """)

    conn.commit()

def main(curr):
    useDB(curr)
    loadCountry(curr)
    loadCustomer(curr)
    loadInvoice(curr)
    loadGenre(curr)
    loadAlbum(curr)
    loadTrack(curr)
    loadInvoiceLine(curr)
    artist_dict = artistDict();
    for key, value in artist_dict.items():
        print(key, ": ", value)


    loadArtist(curr, artist_dict)
    loadAlbumArtist(curr, artist_dict)
    


main(curr)

