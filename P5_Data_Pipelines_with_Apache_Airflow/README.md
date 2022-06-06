# Project 5: Data Pipelines with Apache Airflow

## I - Introduction to the Project

As a Data Engineer for music streaming company Sparkify, my role on this project is to automate ETL pipelines for Sparkify's subscriber user data.
The user data is located in S3 buckets. The resulting warehouse is located in AWS Redshift.

The individual operators are to be designed in such a way that they are as modular as possible and enable subsequent monitoring.

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


## III - Data Warehouse Modelling

Based on the above data, a data warehouse is now to be created in Star Schema. 
This schema is optimal for the later analysis of the data.

Below the different tables can be found:

***Staging Tables***

    staging_events = copied files from the S3 event bucket
        data: artist, auth, first_name, gender, item_in_session, last_name, length, level, location, method,
              page, registration, session_id, song, status, ts, user_agent, user_id

    staging_songs = copied files from the S3 song bucket
        data: num_songs INT, artist_id, artist_latitude, artist_longitude DECIMAL, artist_location, artist_name,
              song_id, title, duration, year

***Fact Table***

    songplays_table = Relevant data extracted from event and song data -> only entries with page = 'NextSong' were used.
        data: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

***Dimension Tables***

    users_table = user data
        data: user_id, first_name, last_name, gender, level
    songs_table = song data
        data: song_id, title, artist_id, year, duration
    artists_table = corresponding artist data
        data: artist_id, name, location, lattitude, longitude
    time_table = Different time units extracted from the timestamp
        data: start_time, hour, day, week, month, year, weekday

## IV - Project Files

All files are located in the `airflow` folder. 

### Subfolder `dags`:

    udac_example_dag.py - Main dag file for airflow. Input for all operators, dag parameters and definition of task dependencies
    
    
### Subfolder `plugins/helpers`    

    sql_queries.py      -  all necessary queries to create and insert queries for the staging and warehouse tables
   
   
### Subfolder `plugins/operators`  

    stage_redshift.py   - creates the staging tables for songs and events and copys the data from S3 to Redshift
    load_fact.py        - creates songplays fact table and insert data
    load_dimension.py   - creates dimension tables (user, songs, arists and time) 

## V - ETL DAG

![]()


## VI - Project Workflow

    1. The IAM role and the Redshift cluster are created and configured via the AWS gateway -> any missing credentials are entered in dwh.cfg
    2. create_tables.py is executed in the console. After that it is checked if all tables were created correctly
    3. After that etl.py is executed. After about 20 minutes all data are entered in the tables. 
       A sample query is executed to check whether the ETL process was executed completely and correctly.
    4. To avoid unnecessary costs, the Redshift cluster will be deleted after the work is completed.
