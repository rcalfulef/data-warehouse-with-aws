""" Queries to create, drop, copy and insert data into the tables."""
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    event_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR(256),
    auth VARCHAR(20),
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length DECIMAL,
    level VARCHAR(10),
    location VARCHAR(256),
    method VARCHAR(10),
    page VARCHAR(30),
    registration DECIMAL,
    sessionId INTEGER,
    song VARCHAR(256),
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INTEGER
)
diststyle auto
sortkey auto;
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR(256),
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR(500),
    artist_name VARCHAR(500),
    song_id VARCHAR(256),
    title VARCHAR(256),
    duration DECIMAL,
    year INTEGER
)        
diststyle auto
sortkey auto;                       
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR(10),
    song_id VARCHAR(256),
    artist_id VARCHAR(256),
    session_id INTEGER,
    location VARCHAR(256),
    user_agent VARCHAR(256)
)
distkey(song_id)
sortkey(start_time);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50), 
    last_name VARCHAR(50),
    gender CHAR,
    level VARCHAR(10)
) 
diststyle all
sortkey(user_id);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(256),
    location VARCHAR(256),
    latitude DECIMAL,
    longitude DECIMAL
)
diststyle all
sortkey(song_id);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(256),
    latitude DECIMAL,
    longitude DECIMAL
)
diststyle all
sortkey(artist_id);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR(10)
)
diststyle all
sortkey(start_time);
"""

# STAGING TABLES

staging_events_copy = """COPY staging_events
    from {0}
    iam_role {1}
    region '{2}'
    json {3}
""".format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("AWS", "REGION"),
    config.get("S3", "LOG_JSONPATH"),
)

staging_songs_copy = """COPY staging_songs
    from {0}
    iam_role {1}
    region '{2}'
    format as json 'auto'
""".format(
    config.get("S3", "SONG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("AWS", "REGION"),
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id ,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
    se.userId as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM staging_events as se, staging_songs as ss
WHERE 
    se.song = ss.title 
    AND se.artist = ss.artist_name
    AND se.userId IS NOT NULL
    AND ss.artist_id IS NOT NULL
    AND se.page = 'NextSong';                  
"""

user_table_insert = """
INSERT INTO users(
    user_id,
    first_name, 
    last_name,
    gender ,
    level)
SELECT DISTINCT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL                  
"""

song_table_insert = """
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE song_id IS NOT NULL
"""

artist_table_insert = """
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
"""

time_table_insert = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    start_time,
    EXTRACT(hour from start_time) as hour,
    EXTRACT(day from start_time) as day,
    EXTRACT(week from start_time) as week,
    EXTRACT(month from start_time) as month,
    EXTRACT(year from start_time) as year,
    EXTRACT(weekday from start_time) as weekday
from songplays 
"""


song_most_played = """
SELECT
    songs.title,
    artists.name,
    count(songplays.song_id) as count
FROM songplays
JOIN songs
ON songplays.song_id = songs.song_id
JOIN artists
ON songplays.artist_id = artists.artist_id
GROUP BY  songs.title, artists.name
ORDER BY count DESC
limit 10;           
"""

artist_with_more_songs = """
SELECT 
    artists.name,
    count(songs.song_id) as count
FROM songs
JOIN artists
ON songs.artist_id = artists.artist_id
GROUP BY artists.name
HAVING count(songs.song_id) > 1
ORDER BY count DESC
limit 10;
"""

artists_most_listened = """
SELECT 
    artists.name as artist,
    count(songplays.artist_id) as count_plays
FROM songplays
JOIN artists
ON songplays.artist_id = artists.artist_id
GROUP BY songplays.artist_id, artists.name
ORDER BY count_plays DESC
limit 10;
"""


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
queries_analytics = [song_most_played, artist_with_more_songs, artists_most_listened]
