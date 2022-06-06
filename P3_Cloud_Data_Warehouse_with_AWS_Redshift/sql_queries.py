import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artists_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_events 
                                (
                                   artist VARCHAR,
                                   auth VARCHAR,
                                   first_name VARCHAR,
                                   gender CHARACTER,
                                   item_in_session INT,
                                   last_name VARCHAR,
                                   length DECIMAL,
                                   level CHARACTER(4),
                                   location VARCHAR,
                                   method CHARACTER(3),
                                   page VARCHAR,
                                   registration DECIMAL,
                                   session_id INT,
                                   song VARCHAR,
                                   status INT,
                                   ts BIGINT,
                                   user_agent VARCHAR,
                                   user_id INT 
                                );
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                    num_songs INT,
                                    artist_id VARCHAR,
                                    artist_latitude DECIMAL,
                                    artist_longitude DECIMAL,
                                    artist_location VARCHAR,
                                    artist_name VARCHAR,
                                    song_id VARCHAR,
                                    title VARCHAR,
                                    duration DECIMAL,
                                    year INT
                                );
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (
                            songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id INT NOT NULL,
                            level CHARACTER(4) NOT NULL,
                            song_id VARCHAR NOT NULL,
                            artist_id VARCHAR NOT NULL,
                            session_id INT NOT NULL,
                            location VARCHAR,
                            user_agent VARCHAR
                            );                           
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (
                        user_id INT NOT NULL PRIMARY KEY,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        gender VARCHAR,
                        level CHARACTER(4) NOT NULL
                        );
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
                        (
                        song_id VARCHAR NOT NULL PRIMARY KEY,
                        title VARCHAR,
                        artist_id VARCHAR NOT NULL,
                        year INT,
                        duration DECIMAL
                        );
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists_table
                        (
                        artist_id VARCHAR NOT NULL PRIMARY KEY,
                        name VARCHAR,
                        location VARCHAR,
                        latitude DECIMAL,
                        longitude DECIMAL
                        );
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                        (
                        start_time TIMESTAMP NOT NULL PRIMARY KEY,
                        hour INT NOT NULL,
                        day INT NOT NULL,
                        week INT NOT NULL,
                        month INT NOT NULL,
                        year INT NOT NULL,
                        weekday INT NOT NULL
                        );
                    """)

# STAGING TABLES

staging_events_copy = ("""
                        COPY staging_events
                        FROM {}
                        IAM_ROLE {}
                        JSON {};
                        """).format(config['S3']['LOG_DATA'], 
                                    config['IAM_ROLE']['ARN'], 
                                    config['S3']['LOG_JSONPATH'])
staging_songs_copy = ("""
                        COPY staging_songs
                        FROM {}
                        IAM_ROLE {}
                        JSON 'auto';
                      """).format(config['S3']['SONG_DATA'], 
                                  config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
                           INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                           SELECT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time, e.user_id, e.level, 
                           s.song_id, s.artist_id, e.session_id, 
                           e.location, e.user_agent
                           FROM staging_events AS e
                           JOIN staging_songs AS s ON e.song = s.title AND e.artist = s.artist_name
                           WHERE e.page = 'NextSong';
                           """)

user_table_insert = ("""
                        INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'; 
                    """)

song_table_insert = ("""
                        INSERT INTO song_table (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs;
                    """)

artist_table_insert = ("""
                          INSERT INTO artists_table (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location,
                          artist_latitude AS latitude, artist_longitude AS longitude
                          FROM staging_songs;
                       """)

time_table_insert = ("""
                        INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
                        SELECT st.start_time, EXTRACT(hour FROM st.start_time) AS hour, EXTRACT(day FROM st.start_time) AS day,
                               EXTRACT(week FROM st.start_time) AS week , EXTRACT(month FROM st.start_time) AS month, EXTRACT(year FROM st.start_time) AS year,
                               EXTRACT(weekday FROM st.start_time) AS weekday
                        FROM   (SELECT DISTINCT
                               TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time
                               FROM staging_events
                               WHERE page = 'NextSong') AS st;
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
