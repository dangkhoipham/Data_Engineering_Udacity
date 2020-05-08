import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artisst"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_events(artist VARCHAR,
                                                                          auth VARCHAR,
                                                                          firstName VARCHAR,
                                                                          gender CHAR,
                                                                          itemInSession INTEGER,
                                                                          lastName VARCHAR,
                                                                          length NUMERIC,
                                                                          level VARCHAR,
                                                                          location VARCHAR,
                                                                          method VARCHAR,
                                                                          page VARCHAR,
                                                                          registratin BIGINT,
                                                                          sessionId INTEGER,
                                                                          song VARCHAR,
                                                                          status INTEGER,
                                                                          ts BIGINT,
                                                                          userAgent VARCHAR,
                                                                          userId INTEGER);
""")

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                                                    num_songs INTEGER,
                                                                    artist_id VARCHAR NOT NULL UNIQUE,
                                                                    artist_latitude NUMERIC,
                                                                    artist_longitude NUMERIC,
                                                                    artist_location VARCHAR,
                                                                    artist_name VARCHAR NOT NULL,
                                                                    song_id VARCHAR NOT NULL,
                                                                    title VARCHAR NOT NULL,
                                                                    duration NUMERIC NOT NULL,
                                                                    year INTEGER);
                             """)

songplay_table_create = ("""
                                CREATE TABLE IF NOT EXISTS songplays (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                                                                    start_time TIMESTAMP,
                                                                    user_id INTEGER NOT NULL,
                                                                    level VARCHAR,
                                                                    song_id VARCHAR NOT NULL,
                                                                    artist_id VARCHAR NOT NULL,
                                                                    session_id INTEGER,
                                                                    location VARCHAR,
                                                                    user_agent VARCHAR)
                                                                    DISTSTYLE KEY
                                                                    DISTKEY(artist_id)
                                                                    SORTKEY(start_time);
""")

user_table_create = ("""
                                CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY,
                                                                first_name VARCHAR NOT NULL,
                                                                last_name VARCHAR NOT NULL,
                                                                gender CHAR,
                                                                level VARCHAR)
                                                                SORTKEY(user_id);
""")

song_table_create = ("""
                                CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR NOT NULL,
                                                                 title VARCHAR NOT NULL,
                                                                 artist_id VARCHAR NOT NULL,
                                                                 year INTEGER,
                                                                 duration NUMERIC)
                                                                 SORTKEY(song_id);
""")

artist_table_create = ("""
                                CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR PRIMARY KEY,
                                                                   name VARCHAR,
                                                                   location VARCHAR,
                                                                   latitude NUMERIC,
                                                                   longitude NUMERIC)
                                                                   SORTKEY (artist_id);
""")

time_table_create = ("""
                                CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP PRIMARY KEY,
                                                                hour INTEGER,
                                                                day INTEGER,
                                                                week INTEGER,
                                                                month INTEGER,
                                                                year INTEGER,
                                                                weekday INTEGER)
                                                                DISTSTYLE KEY
                                                                DISTKEY (start_time)
                                                                SORTKEY(start_time);
""")

#COPY STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = (""" 
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES created by INSERT rows from STAGING TABLES to final TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent )
                            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL'1 second', userId,
                                            level, song_id, artist_id, sessionId, location, userAgent
                            FROM staging_events se
                            JOIN staging_songs ss
                            ON se.artist = ss.artist_name
                            WHERE se.page = 'NextSong'

""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
                         SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events
                         WHERE userId IS NOT NULL
                         AND page = 'NextSong';
""")

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id, year, duration)
                         SELECT DISTINCT song_id, title, artist_id, year, duration 
                         FROM staging_songs;
""")

artist_table_insert = (""" INSERT INTO artists(artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT t.stime, EXTRACT(hour FROM t.stime), EXTRACT(day FROM t.stime), 
                                        EXTRACT(week FROM t.stime), EXTRACT(month FROM t.stime), 
                                        EXTRACT(year FROM t.stime), EXTRACT(weekday FROM t.stime)
                        FROM (
                              SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as stime
                              FROM staging_events
                              ) t
                        ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
