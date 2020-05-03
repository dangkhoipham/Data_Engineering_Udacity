# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# To reviewer: I agree that we should make artist_id NOT NULL, but in this project, we do not have enough
# data of artists in the log file. In the songplays table, we have 6820 rows, but only one row has NOT NULL artist_id.
# Therefore, I did not place NOT NULL for the artist_id; otherwise the etl.py cannot run.
songplay_table_create = """CREATE TABLE songplays(songplay_id SERIAL, 
                                                  start_time timestamp, 
                                                  user_id varchar NOT NULL,
                                                  artist_id varchar, 
                                                  session_id int, 
                                                  location varchar, 
                                                  user_agent varchar,
                                                  PRIMARY KEY(songplay_id))"""

user_table_create = """CREATE TABLE users(user_id int PRIMARY KEY, 
                                         first_name varchar NOT NULL, 
                                         last_name varchar NOT NULL, 
                                         gender varchar, 
                                         level varchar)"""

song_table_create = """CREATE TABLE songs(song_id varchar PRIMARY KEY, 
                                          title varchar NOT NULL,
                                          artist_id varchar NOT NULL, 
                                          year int, 
                                          duration numeric NOT NULL)"""

artist_table_create = """CREATE TABLE artists(artist_id varchar PRIMARY KEY,
                                              name varchar NOT NULL, 
                                              location varchar, 
                                              latitude varchar, 
                                              longitude varchar)"""

time_table_create = """CREATE TABLE time(start_time timestamp PRIMARY KEY, 
                                        hour int, 
                                        day int, 
                                        week int, 
                                        month int, 
                                        year int, 
                                        weekday int)"""

# INSERT RECORDS

songplay_table_insert = "INSERT INTO songplays(start_time, user_id, artist_id, session_id, location, user_agent) \
                        VALUES(%s,%s,%s,%s,%s,%s) \
                        ON CONFLICT(songplay_id) DO NOTHING;"

user_table_insert = """INSERT INTO users(user_id, first_name, last_name, gender, level) 
                    VALUES(%s,%s,%s,%s,%s)
                    ON CONFLICT(user_id) DO UPDATE 
                    SET level = EXCLUDED.level;"""

song_table_insert = """INSERT INTO songs(song_id, title, artist_id, year, duration) 
                       VALUES(%s,%s,%s,%s,%s)
                       ON CONFLICT(song_id) DO NOTHING;"""

artist_table_insert = """INSERT INTO artists(artist_id, name, location, latitude, longitude) 
                         VALUES (%s, %s, %s, %s, %s)
                         ON CONFLICT(artist_id) DO NOTHING;"""


time_table_insert = """INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
                       VALUES (%s,%s,%s,%s,%s,%s,%s) 
                       ON CONFLICT(start_time) DO NOTHING;"""

# FIND SONGS

song_select = """SELECT songs.song_id, artists.artist_id 
                 FROM songs JOIN artists 
                 ON songs.artist_id = artists.artist_id 
                 WHERE title = %s AND name = %s AND duration = %s"""

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
