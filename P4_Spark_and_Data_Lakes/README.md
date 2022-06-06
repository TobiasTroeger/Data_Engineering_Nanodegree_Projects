# Project_4: Data Lake

## I - Introduction to the Project

Music streaming startup Sparkyfiy has expanded its operations along with its user base and the songs available in its database. The company is now looking to move its processes to the cloud to improve future growth and scalability of operations.

Sparkyfies data resides in S3, in the form of JSON logs of user activity in the app as well as metadata of the songs, also as JSON logs.

As a Data Engineer, my job is to build a data lake in AWS based on the S3 data.
For this purpose, an ETL pipeline has to be created to extract the data from the S3 buckets and store it in a new S3 Bucket. Based on this data, a fact table and several dimensional tables are to be created in the same new S3 bucket.

The goal is to give Sparkyfie's data analysis team a better insight into user activity on their app.
The performance of the new database can be tested with queries that Sparkyfie's data analysis team will provide and match against their expected results.


## II - Datasets

The two datasets we are working with can be found on S3 with the following links:

    Song data: s3://udacity-dend/song_data
    
    Log data: s3://udacity-dend/log_data
    -> Log data json path: s3://udacity-dend/log_json_path.json


### Song Data

The song data contains metadata about each song and its artist. The folder is partitioned by the first 3 letters of each song. 
The source for this dataset was a subset of the "Million Song Dataset".

As an example, here are the links to two files from the dataset:

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

Example datastructure of the file TRAABJL12903CDCF1A.json:

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", 
    "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


### Log Data

Based on the song data, this data was created using the "Eventsim" program. This is a compilation of user data for a theoretical music streaming app. 
This data is grouped by time (month/year) within the folder. 

Here again is an example of the data with the associated links:


    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json

Example of the log file: 2018-11-12-events.json:

![](image_log_data.png)


## III - Data Lake

Based on the above data, a data lake is now to be created in Star Schema. 
This schema is optimal for the later analysis of the data. 

Therefor a

Below the different tables can be found:

***Staging Tables***

    log_staging_schema = extracts the raw data from a S3 bucket, loads it in a new S3 bucket
        Data Schema: artist, auth, first_name, gender, item_in_session, last_name, length, level, location, method,
              page, registration, session_id, song, status, ts, user_agent, user_id

    song_staging_schema = extracts the raw data from a S3 bucket, loads it in a new S3 bucket
        data: num_songs INT, artist_id, artist_latitude, artist_longitude, artist_location, artist_name,
              song_id, title, duration, year

***Fact Table***

    songplays_table = Relevant data extracted from log and song tables -> only entries with page = 'NextSong' were used.
        data: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

***Dimension Tables***

    users_table = user data
        data: user_id, first_name, last_name, gender, level
    songs_table = song data
        data: song_id, title, artist_id, year, duration
    artists_table = corresponding artist data
        data: artist_id, name, location, lattitude, longitude
    time_table = ts is transformed in a timestamp and from this in datetime. Different time data is then extracted from the datetime
        data: start_time, hour, day, week, month, year, weekday

## IV - Project Files

Files in the project folder:

    etl.py           -  copies the data from S3 into the staging tables and then transforms them to create the star schema
    dl.cfg           -  credentials for AWS cloud
    README.md        -  gives a short discription of the project
    

## V - ETL Pipeline

    1. The IAM role is created with S3AllAccess as well as a EMR cluster. The dl.cfg file is updated with the latest login credentials.
    2. The S3 bucket with a representative name is chosen and the etl.py is updated with the s3a path for the output_data.
    2. After that etl.py is executed. It does the following:
        2.1 creates the spark session
        2.2 extracts the song and log files and load them in the new S3 bucket
        2.3 transforms the data from song and log files in the new tables songplays, artist, user, song and time.
    3. To avoid unnecessary costs, the S3 bucket and EMR cluster will be deleted after the work is completed.
