import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Description: Function to parse files in log_data directory and input 
    values in the tables songs and artists

    Arguments:
        cur: the cursor object. 
        filepath: json log_data filepath

    Returns:
        None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    for i, row in df[['song_id', 'title', 'artist_id', 'year', 'duration']].iterrows():
        cur.execute(song_table_insert, list(row))
    
    # insert artist record
    for i, row in df[['artist_id', 'artist_name' ,'artist_location', 'artist_latitude' ,'artist_longitude']].iterrows():
        cur.execute(artist_table_insert, list(row))


def process_log_file(cur, filepath):
    """
    Description: Function to parse files in log_data directory and input 
    values in the tables time, users and songplay

    Arguments:
        cur: the cursor object. 
        filepath: json log_data filepath

    Returns:
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = df[['ts']].copy()
    t['datetime'] = pd.to_datetime(t['ts'], unit='ms')

    # insert time data records
    time_data = list((t['ts'], t['datetime'].dt.hour, t['datetime'].dt.day, t['datetime'].dt.isocalendar().week, t['datetime'].dt.month, t['datetime'].dt.year, t['datetime'].dt.weekday))
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level', 'ts']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, int(row.length)))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)        

def process_data(cur, conn, filepath, func):
    """
    Description: Iterate over each json file in input parameter 'filepath' including subdirectories.
    Call 'func' to process each file

    Arguments:
        cur: the cursor object. 
        conn: database connection.
        filepath: directory root path where all json input files are stored. 
        func: function to be called to process each file from.

    Returns:
        None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: Main function, it processes the files from song_data and log_data
    and input into the postgres database
    """

    conn = psycopg2.connect("host=postgres dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()