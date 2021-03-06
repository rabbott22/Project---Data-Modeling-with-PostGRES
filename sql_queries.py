# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id bigserial PRIMARY KEY, start_time timestamp REFERENCES time (start_time) NOT NULL, user_id int REFERENCES users (user_id) NOT NULL, level char(4), song_id varchar REFERENCES song (song_id), artist_id varchar REFERENCES artist (artist_id), session_id int, location varchar, user_agent varchar)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender char(1), level char(4))""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar REFERENCES artist (artist_id), year int, duration numeric NOT NULL)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, latitude double precision, longitude double precision)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day smallint, week smallint, month smallint, year int, weekday smallint)""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET level = EXCLUDED.level""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT song_pkey DO NOTHING""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT artist_pkey DO NOTHING""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT time_pkey DO NOTHING""")

# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id FROM song s JOIN artist a ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s""")

# QUERY LISTS

create_table_queries = [artist_table_create, song_table_create, user_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]