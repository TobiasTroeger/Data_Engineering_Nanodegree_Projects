# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES 
# Schema: Star Schema
# Fact Tables: 1 / songplays
# Dimension Tables: 4 / users, songs, artists, time 

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id SERIAL PRIMARY KEY,
                                start_time BIGINT NOT NULL,
                                user_id INT NOT NULL,
                                level VARCHAR NOT NULL,
                                song_id VARCHAR, 
                                artist_id VARCHAR,
                                session_id INT NOT NULL, 
                                location VARCHAR, 
                                user_agent VARCHAR);
                        """)

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id INT PRIMARY KEY NOT NULL, 
                            first_name VARCHAR,
                            last_name VARCHAR,
                            gender VARCHAR,
                            level VARCHAR NOT NULL);
                    """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songs (
                            song_id VARCHAR PRIMARY KEY NOT NULL,
                            title VARCHAR,
                            artist_id VARCHAR NOT NULL,
                            year INT,
                            duration FLOAT);
                    """)

artist_table_create = ("""
                          CREATE TABLE IF NOT EXISTS artists (
                              artist_id VARCHAR PRIMARY KEY NOT NULL,
                              name VARCHAR NOT NULL,
                              location VARCHAR,
                              latitude FLOAT,
                              longitude FLOAT);
                        """)

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time (
                            start_time BIGINT PRIMARY KEY NOT NULL,
                            hour INT NOT NULL,
                            day INT NOT NULL,
                            week INT NOT NULL,
                            month INT NOT NULL,
                            year INT NOT NULL,
                            weekday INT NOT NULL);
                    """)

# INSERT RECORDS
# insert queries for all 5 tables

songplay_table_insert = ("""
                            INSERT INTO songplays
                            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (songplay_id) DO NOTHING;
                        """)

user_table_insert = ("""
                        INSERT INTO users
                        (user_id, first_name, last_name, gender, level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
                    """)


song_table_insert = ("""
                        INSERT INTO songs 
                        (song_id, title, artist_id, year, duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) DO NOTHING;
                    """)

artist_table_insert = ("""
                          INSERT INTO artists 
                          (artist_id, name, location, latitude, longitude)
                          VALUES (%s, %s, %s, %s, %s)
                          ON CONFLICT (artist_id) DO NOTHING;
                    """)


time_table_insert = ("""
                        INSERT INTO time 
                        (start_time , hour, day, week, month, year, weekday)
                        VALUES(%s, %s ,%s ,%s ,%s ,%s, %s)
                        ON CONFLICT (start_time) DO NOTHING;
                    """)

# FIND SONGS
# Query for song_id and artist_id from the joint table artists and songs
# -> filtered by song title, artist name and song duration

song_select = ("""
                  SELECT songs.song_id, artists.artist_id
                  FROM songs
                  JOIN artists ON (songs.artist_id = artists.artist_id)
                  WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
              """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]