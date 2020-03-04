import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
artist VARCHAR,
auth VARCHAR NOT NULL,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length numeric,
level VARCHAR,
location VARCHAR,
method VARCHAR NOT NULL,
page VARCHAR NOT NULL,
registration numeric,
sessionId INTEGER,
song VARCHAR sortkey distkey,
status INTEGER NOT NULL,
ts VARCHAR NOT NULL,
userAgent VARCHAR,
userId VARCHAR NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
song_id VARCHAR NOT NULL, 
num_songs INTEGER,
artist_id VARCHAR NOT NULL,
artist_latitude numeric,
artist_longitude numeric,
artist_location VARCHAR,
artist_name VARCHAR,
title VARCHAR NOT NULL sortkey distkey,
duration numeric NOT NULL,
year INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id BIGINT IDENTITY(0,1) NOT NULL sortkey,
start_time timestamp NOT NULL,
user_id BIGINT NOT NULL,
level VARCHAR,
song_id VARCHAR NOT NULL distkey,
artist_id VARCHAR NOT NULL,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
user_id BIGINT PRIMARY KEY NOT NULL sortkey, 
first_name VARCHAR NOT NULL,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR NOT NULL
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs(
song_id VARCHAR PRIMARY KEY NOT NULL sortkey,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year INTEGER NOT NULL,
duration numeric
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists(
artist_id VARCHAR NOT NULL sortkey,
name VARCHAR NOT NULL,
location VARCHAR,
lattitude numeric, 
longitude numeric
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE time(
start_time VARCHAR NOT NULL sortkey,
hour VARCHAR NOT NULL,
day VARCHAR NOT NULL,
week VARCHAR NOT NULL,
month VARCHAR NOT NULL,
year VARCHAR NOT NULL,
weekday VARCHAR NOT NULL
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from '{}'
credentials 'aws_iam_role={}'
format as json {} region 'us-west-2'
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2'
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(user_id,song_id,artist_id,start_time,level,
session_id,location,user_agent) SELECT e.userId::INTEGER,s.song_id,s.artist_id,(TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ') as ts,e.level,e.sessionId, 
e.location,e.userAgent from staging_events e inner join staging_songs s on 
e.song=s.title; 
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level) SELECT  distinct(e.userId::INTEGER),e.firstName,e.lastName,e.gender,e.level
from staging_events e 
where e.ts = (SELECT  max(se.ts)
from staging_events se where se.userId=e.userId) AND
(length(trim(e.userId)) > 0);  
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration) SELECT s.song_id,s.title,s.artist_id,s.year,s.duration
from staging_songs s;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,lattitude,longitude) SELECT distinct (s.artist_id),s.artist_name,s.artist_location,
s.artist_latitude,s.artist_longitude from staging_songs s 
where s.year=(select max(ss.year) from staging_songs ss where s.artist_id=ss.artist_id);
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday) SELECT 
DISTINCT (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ') as ts,
EXTRACT(hour FROM (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ')),
EXTRACT(day FROM (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ')),
EXTRACT(week FROM (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ')),
EXTRACT(month FROM (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ')),
EXTRACT(year FROM (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second ')),
EXTRACT(weekday FROM (TIMESTAMP 'epoch' + CAST(e.ts AS BIGINT)/1000 * INTERVAL '1 Second '))
from staging_events e;
""") 

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create] 
drop_table_queries = [staging_events_table_drop,staging_songs_table_drop,songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]  
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
