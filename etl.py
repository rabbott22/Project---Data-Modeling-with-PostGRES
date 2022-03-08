import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from time import strftime


def process_song_file(cur, filepath):
    """
    - Read song and artist data from Sparkify JSON song files in file 
    system. 
    
    - Load data into the corresponding "song" and "artist" tables
    in the database.

    Arguments:
    cur -- database connection cursor
    filepath -- absolute path to the source JSON files 
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0].tolist()
    '''Remove DOCSTRING comments to enable logging
    # start log 
    af = open("artist_record.log", "a")
    af.write(strftime("%Y-%m-%d %H:%M:%S") + " " + artist_table_insert + '\n')
    new_ad = [str(i) for i in artist_data]
    af.write(' '.join(new_ad) + '\n')
    af.close()
    # end log
    '''
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values[0].tolist()
    '''Remove DOCSTRING comments to enable logging
    # start log -  
    sf = open("song_record.log", "a")
    sf.write(strftime("%Y-%m-%d %H:%M:%S") + " " + song_table_insert + '\n')
    new_sd = [str(i) for i in song_data]
    sf.write(' '.join(new_sd) + '\n')
    sf.close()
    # end log
    '''
    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
    """
    - Read songplay, time, and user data from Sparkify JSON log files 
    in file system.
    
    - Load data into the corresponding "songplay", "time", and "users" tables in the database.

    Arguments:
    cur -- database connection cursor
    filepath -- absolute path to the source JSON files
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df.page == "NextSong")]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_dict = {}
    for i, label in enumerate(column_labels):
        time_dict[label] = time_data[i]
    time_df = pd.DataFrame.from_dict(time_dict)

    '''Remove DOCSTRING comments to enable logging
    # start log 
    tf = open("time_record.log", "a")
    '''
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        '''
        # continue log 
        tf.write(strftime("%Y-%m-%d %H:%M:%S") + " " + time_table_insert + '\n')
        new_td = [str(i) for i in list(row)]
        tf.write(' '.join(new_td) + '\n')
    tf.close()
    # end log
    '''
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Create a list of absolute file paths to the Sparkify JSON 
    song and log files in the file system.
    
    - Call the function corresponding to the file type for processing.

    Arguments:
    cur -- database connection cursor
    conn -- database connection
    filepath -- relative path to the source JSON files
    func -- function to call, either process_song_file or process_log_file

    Returns:
    For each file type, it prints to stdout:
        "x files found in <relative filepath>"
        "n/x files processed."
        (where x = total files found, n = incremental file number) 
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
    - Create a connection to the database.
    
    - Generate a database cursor.
    
    - Call the song and log file processing functions.
    
    - Close the database connection.
    """    
    # Connect to database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # Generate database cursor
    cur = conn.cursor()

    # Call process_song_file and process_log_file functions, defined above
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Close database connection
    conn.close()

if __name__ == "__main__":
    main()