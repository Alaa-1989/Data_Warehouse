import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (event_id INTEGER IDENTITY(0,1),
                                           artist_id VARCHAR(255),
                                           auth VARCHAR(255),
                                           first_name VARCHAR(255),
                                           last_name VARCHAR(255), 
                                           gender VARCHAR(1),
                                           item_in_session INTEGER,
                                           song_length FLOAT,
                                           level VARCHAR(255),
                                           location VARCHAR(255),
                                           method VARCHAR(50),
                                           page VARCHAR(50),
                                           registration VARCHAR(50),
                                           session_id BIGINT,
                                           song_title VARCHAR(255),
                                           status INTEGER,
                                           ts VARCHAR(50),
                                           user_agent TEXT,
                                           user_id INTEGER,
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (song_id VARCHAR(100) PRIMARY KEY,
                                          title VARCHAR(255),
                                          duration NUMERIC,
                                          year INTEGER,
                                          num_songs INTEGER,
                                          artist_id VARCHAR(100),
                                          artist_name VARCHAR(255),
                                          artist_latitude NUMERIC,
                                          artist_longitude NUMERIC,
                                          artist_location VARCHAR(255)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
                                      user_id INTEGER NOT NULL REFERENCES users(user_id), 
                                      song_id VARCHAR NOT NULL REFERENCES songs(song_id) distkey, 
                                      artist_id VARCHAR NOT NULL REFERENCES artists(artist_id), 
                                      session_id INTEGER NOT NULL, 
                                      start_time TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey, 
                                      level VARCHAR NOT NULL, 
                                      location VARCHAR NOT NULL, 
                                      user_agent TEXT NOT NULL)
""")
                                                                            
user_table_create = ("""
 CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, 
                                   first_name VARCHAR(255), 
                                   last_name VARCHAR(255), 
                                   gender VARCHAR(1), 
                                   level VARCHAR(50))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR(100) PRIMARY KEY, 
                                  artist_id VARCHAR(100) NOT NULL, 
                                  title VARCHAR(255), 
                                  year INTEGER, 
                                  duration NUMERIC)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR(100) PRIMARY KEY, 
                                    name VARCHAR(255), 
                                    location VARCHAR(255), 
                                    latitude NUMERIC, 
                                    longitude NUMERIC)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, 
                                 hour INTEGER, 
                                 day INTEGER, 
                                 week INTEGER, 
                                 month INTEGER, 
                                 year INTEGER, 
                                 weekday VARCHAR(50))
""")


# STAGING TABLES

staging_events_copy = (""" 
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""").format(CONFIG["S3"]["LOG_DATA"], 
            CONFIG["IAM_ROLE"]["ARN"], 
            CONFIG["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(CONFIG["S3"]["SONG_DATA"], 
            CONFIG["IAM_ROLE"]["ARN"])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
       se.user_id,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.location,
       se.user_agent
FROM staging_events se
LEFT JOIN staging_songs ss  ON (se.song_title = ss.title)
AND (se.artist_id = ss.artist_id)
WHERE page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
       se.user_id,
       se.first_name,
       se.last_name,
       se.gender,
       se.level
FROM staging_events se
WHERE user_id NOT IN (SELECT DISTINCT user_id FROM users)
AND SE.page = 'NextSong'
GROUP BY user_id, first_name, last_name, gender, level
ORDER BY user_id;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
       ss.song_id,
       ss.title,
       ss.artist_id,
       ss.year,
       ss.duration
FROM staging_songs ss
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")


artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
        ss.artist_id,
        ss.artist_name            AS name,
        ss.artist_location        AS location,
        ss.artist_latitude        AS latitude,
        ss.artist_longitude       AS longitude
FROM staging_songs ss
WHERE ss.artist_id NOT IN (SELECT DISTINCT ss.artist_id FROM artists)
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        EXTRACT(hour from start_time)        AS hour,
        EXTRACT(day from start_time)         AS day,
        EXTRACT(week from start_time)        AS week,
        EXTRACT(month from start_time)       AS month,
        EXTRACT(year from start_time)        AS year, 
        EXTRACT(weekday from start_time)     AS weekday        
FROM staging_events se     
WHERE se.page = 'NextSong';

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
