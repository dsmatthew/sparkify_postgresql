import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Parameters
    ----------
    cur : cursor object
        The cursor of current postgresql connection.
    filepath : string
        contains the local path the folder which contains the json files.
    
    Return
    ------
    /
    
    Objective
    ---------
    Ingests song data of Sparkify into PostgreSQL database.
    
    '''
    # open song file
    df = pd.read_json(filepath, lines=True, orient='columns')

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Parameters
    ----------
    cur : cursor object
        The cursor of current postgresql connection.
    filepath : string
        contains the local path the folder which contains the json files.
    
    Return
    ------
    /
    
    Objective
    ---------
    Ingests user action log data of Sparkify into PostgreSQL database.
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.DataFrame(pd.to_datetime(df['ts'], unit='ms'))
    df['ts'] = pd.DataFrame(pd.to_datetime(df['ts'], unit='ms')) # required for later processing
    
    # insert time data records
    time_data = [t['ts'], t['ts'].dt.hour, t['ts'].dt.day, t['ts'].dt.weekofyear, t['ts'].dt.month\
                 , t['ts'].dt.year, t['ts'].dt.weekday]
    column_labels = ['ts', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']

    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    #user_df = user_df.replace('', np.nan) # fix for the string

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Parameters
    ----------
    cur : cursor object
        The cursor of current PostgreSQL connection.
    conn : connection object
        The connection object of current PostgreSQL connection.
    func : Function object
        Executes the submitted function.
    Return
    ------
    /
    
    Objective
    ---------
    Processes all submitted data objects within the defined functions.
    '''
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
