import os
import glob
import pandas as pd 
import psycopg2
from sql_queries import *

def process_song_file(cur, file_path):
    """
    Process songs file and insert into the Postgres database
    :param cur: cursor reference
    :param file_path: complete file path for the file to load
    """
    # open song file 
    df = pd.DataFrame([pd.read_json(file_path, typ='series', convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
    
    print(f"Records inserted for file {file_path}")

def process_log_file(cur, file_path):
    """
    Process event log files and insert records into the Postgres database
    :param cur: cursor reference
    :param file_path: complete file path for the file to load
    """
    # open log file
    df = df = pd.read_json(file_path, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].astype({'ts': 'datetime64[ms]'})

    #convert timestamp column to datetime 
    t = pd.Series(df['ts'], index=df.index)

    # insert time data records
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_data = []
    for data in t:
        time_data.append([data, data.hour, data.day, data.week, data.month, data.year, data.day_name()])

    time_df = pd.DataFrame.from_records(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records

    for i, row in df.iterrows():
        # get songid and artistid from song table and artist table
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay records
        songplay_data = (row.ts, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, file_path, func):
    """
    Drive function to load data from songs and event log files into Postgres database.
    :param cur: a cursor preference
    :param conn: database connection reference
    :param file_path: parent directoty where the files exists
    :param func: function to call
    """
    # get all files matching extension from directory 
    all_files = []
    for root, dir, files in os.walk(file_path):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    # get total number of files found 
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, file_path))
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed'.format(i, num_files))

def main():
    """
    Driver funtion for loading songs and logs data into Postgres database 
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=test123")
    cur = conn.cursor()

    process_data(cur, conn, 'data/song_data', func=process_song_file)
    process_data(cur, conn, 'data/log_data', func=process_log_file)

    conn.close()

if __name__ == '__main__':
    main()
    print("\n\n Finished processing!!!\n\n")