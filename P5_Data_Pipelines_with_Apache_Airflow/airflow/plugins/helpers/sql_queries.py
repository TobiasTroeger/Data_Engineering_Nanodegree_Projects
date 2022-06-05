class SqlQueries:
    
    """
    Contains all SQL queries for creating tables, insert data and the quality check.
    """
    
    create_staging_events = ("""
        CREATE TABLE IF NOT EXISTS staging_events 
            (
	        artist VARCHAR,
	        auth VARCHAR,
	        first_name VARCHAR,
	        gender VARCHAR,
	        item_in_session int,
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
    
    create_staging_songs = ("""
        CREATE TABLE IF NOT EXISTS staging_songs 
            (
	        num_songs INT,
	        artist_id VARCHAR,
	        artist_name VARCHAR,
	        artist_latitude DECIMAL,
	        artist_longitude DECIMAL,
	        artist_location VARCHAR,
	        song_id VARCHAR,
	        title VARCHAR,
	        duration DECIMAL,
	        year INT
            );   
    """)
    
    create_songplays_table = ("""
        CREATE TABLE IF NOT EXISTS songplays 
            (
	        songplay_id VARCHAR NOT NULL,
	        start_time TIMESTAMP NOT NULL,
	        user_id INT NOT NULL,
	        level CHARACTER(4),
	        song_id VARCHAR,
	        artist_id VARCHAR,
	        session_id INT,
	        location VARCHAR,
	        user_agent VARCHAR
            );      
    """)
    
    create_user_table = ("""
        CREATE TABLE IF NOT EXISTS user_table 
            (
	        user_id INT NOT NULL PRIMARY KEY,
	        first_name VARCHAR,
	        last_name VARCHAR,
	        gender VARCHAR,
	        level CHARACTER(4) NOT NULL
            );
    """)
    
    create_songs_table = ("""
        CREATE TABLE IF NOT EXISTS songs_table 
            (
	        song_id VARCHAR NOT NULL PRIMARY KEY,
	        title VARCHAR,
	        artist_id VARCHAR NOT NULL,
	        year INT,
	        duration DECIMAL
            );
    """)
    
    create_artists_table = ("""
        CREATE TABLE IF NOT EXISTS artists_table 
            (
	        artist_id VARCHAR NOT NULL PRIMARY KEY,
	        name VARCHAR,
	        location VARCHAR,
	        lattitude DECIMAL,
	        longitude DECIMAL
            );  
    """)
    
    create_time_table = ("""
        CREATE TABLE IF NOT EXISTS time_table 
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
    
    songplay_table_insert = ("""
        SELECT
                md5(COALESCE(events.session_id, 0) || events.start_time) songplay_id,
                events.start_time, 
                events.user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct user_id, first_name, last_name, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    test_sql_query = ("""
                    SELECT COUNT(*) FROM songs_table
                    WHERE song_id = null; 
    """)
