import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# Reading some useful constants
AMI_ROLE = config("IAM_ROLE", "ARN")
LOG_DATA = config("S3", "LOG_DATA")
LOG_DATA_FORMAT = config("S3", "LOG_JSONPATH")
SONG_DATA = config("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "staging_events"
staging_songs_table_drop = "staging_songs"
songplay_table_drop = "songplay"
user_table_drop = "user"
song_table_drop = "song"
artist_table_drop = "artist"
time_table_drop = "time"

# CREATE TABLES

# STAGING TABLE
staging_events_table_create = """
    CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
    )
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id IDENTITY(0,1) NOT NULL,
    start_time bigint NOT NULL,
    user_id int NOT NULL,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text,
    
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(song_id) REFERENCES song(song_id),
    FOREIGN KEY(artist_id) REFERENCES artist(artist_id),
    FOREIGN KEY(start_time) REFERENCES time(start_time)
    
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS user (
    user_id int NOT NULL,
    first_name text,
    last_name text,
    gender text,
    level text,

    PRIMARY KEY(user_id)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS song (
    song_id text NOT NULL,
    title text,
    artist_id text,
    year int,
    duration real,

    PRIMARY KEY(song_id)
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist (
    artist_id text NOT NULL,
    name text,
    location text,
    latitude text,
    longitude text,

    PRIMARY KEY(artist_id)
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time bigint NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday text,

    PRIMARY KEY(start_time)
)
"""

# STAGING TABLES

staging_events_copy = f"""
    copy staging_events from {LOG_DATA} 
    credentials 'aws_iam_role={AMI_ROLE}' 
    region 'us-west-2' format as JSON {LOG_DATA_FORMAT} 
    timeformat as 'epochmillisecs';
"""

staging_songs_copy = f"""
    copy staging_songs from {SONG_DATA}
    credentials 'aws_iam_role={AMI_ROLE}'  
    region 'us-west-2' format as JSON 'auto'
"""

# FINAL TABLES

songplay_table_insert = """

"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)  as user_id,
            firstName  as first_name,
            lastName  as last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  ==  'NextSong';
"""

song_table_insert = """
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id) as song_title,
       title,
       artist_id,
       year,
       duration
FROM  staging_songs
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) as artist_id, 
       artist_name as name,
       artist_location as location,
       artist_latitude  as latitude,
       artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
       
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(start_time) as start_time,
       EXTRACT(hour FROM start_time) as hour,
       EXTRACT(day FROM start_time) as day,
       EXTRACT(week FROM start_time) as week,
       EXTRACT(month FROM start_time) as month,
       EXTRACT(year FROM start_time) as year,
       EXTRACT(dayofweek FROM start_time) as weekday
FROM  songplay
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
