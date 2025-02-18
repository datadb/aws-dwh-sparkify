import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays_fact;"
user_table_drop = "DROP TABLE IF EXISTS users_dim;"
song_table_drop = "DROP TABLE IF EXISTS songs_dim;"
artist_table_drop = "DROP TABLE IF EXISTS artists_dim;"
time_table_drop = "DROP TABLE IF EXISTS time_dim;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
    (
        artist          VARCHAR(255),
        auth            VARCHAR(15),
        first_name      VARCHAR(50),
        gender          VARCHAR(13),
        item_in_session INTEGER,
        last_name       VARCHAR(50),
        length          FLOAT4,
        level           VARCHAR(10),
        location        TEXT,
        method          VARCHAR(10),
        page            VARCHAR(20),
        registration    DOUBLE PRECISION,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
    (
        num_songs          INTEGER,
        artist_id          TEXT,
        artist_latitude    DOUBLE PRECISION,
        artist_longitude   DOUBLE PRECISION,
        artist_location    VARCHAR(MAX),
        artist_name        VARCHAR(MAX),
        song_id            TEXT,
        title              VARCHAR(255),
        duration           FLOAT4,
        year               INTEGER
    );
""")



songplay_table_create = ("""
CREATE TABLE songplays_fact
    (
        songplay_id    BIGINT         IDENTITY(0, 1)    PRIMARY KEY, 
        start_time     TIMESTAMP                        sortkey,
        user_id        INTEGER,
        level          VARCHAR(10),
        song_id        TEXT                             distkey,
        artist_id      TEXT,
        session_id     INTEGER,
        location       TEXT,
        user_agent     TEXT
    );
""")


user_table_create = ("""
CREATE TABLE users_dim
    (
        user_id        INTEGER        PRIMARY KEY        sortkey,
        first_name     VARCHAR(50),
        last_name      VARCHAR(50),
        gender         VARCHAR(13),
        level          VARCHAR(10)   
    );
""")


song_table_create = ("""
CREATE TABLE songs_dim
    (
        song_id        TEXT           PRIMARY KEY        distkey sortkey,
        title          VARCHAR(255),
        artist_id      TEXT,
        year           INTEGER,
        duration       FLOAT4
    );
""")


artist_table_create = ("""
CREATE TABLE artists_dim
    (
        artist_id          TEXT          PRIMARY KEY      sortkey, 
        name               VARCHAR(MAX),
        location           VARCHAR(MAX),
        latitude           DOUBLE PRECISION,
        longitude          DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE time_dim
    (
        start_time     TIMESTAMP      PRIMARY KEY          sortkey,
        hour           INTEGER, 
        day            INTEGER,
        week           INTEGER,
        month          INTEGER,
        year           INTEGER,
        weekday        INTEGER
    );
""")


# STAGING TABLES

staging_events_copy = (
""" copy {} from {}
    credentials 'aws_iam_role={}'
    format as json {} region {}
    timeformat as 'epochmillisecs' """).format(
        'staging_events',
        config['S3']['LOG_DATA'],
        config['IAM_ROLE']['ARN'],
        config['S3']['LOG_JSONPATH'],
        config['S3']['REGION']
)


staging_songs_copy = (
""" copy {} from {}
    credentials 'aws_iam_role={}'
    json 'auto' region {} """).format(
        'staging_songs',
        config['S3']['SONG_DATA'],
        config['IAM_ROLE']['ARN'],
        config['S3']['REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays_fact (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM
    staging_events se
LEFT JOIN
    staging_songs ss ON
        se.song = ss.title AND
        se.artist = ss.artist_name AND
        se.page = 'NextSong' AND
        se.length = ss.duration;
""")

user_table_insert = ("""
INSERT INTO users_dim (user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT (user_id) AS user_id,
    first_name,
    last_name,
    gender,
    level
FROM
    staging_events
WHERE
    user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs_dim (song_id, title, artist_id, year, duration)
SELECT DISTINCT (song_id) AS song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM
    staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists_dim (artist_id, name, location, latitude, longitude)
SELECT DISTINCT (artist_id) AS artist_id,
    artist_name,
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM
    staging_songs; 
""")

time_table_insert = ("""
INSERT INTO time_dim (start_time, hour, day, week, month, year, weekday)
WITH timestamp_conversion AS (
    SELECT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS ts_converted
    FROM staging_events
)
SELECT DISTINCT(ts_converted) AS start_time,
    EXTRACT(HOUR FROM ts_converted) AS hour,
    EXTRACT(DAY FROM ts_converted) AS day,
    EXTRACT(WEEK FROM ts_converted) AS week,
    EXTRACT(MONTH FROM ts_converted) AS month,
    EXTRACT(YEAR FROM ts_converted) AS year,
    EXTRACT(DOW FROM ts_converted) AS weekday
FROM timestamp_conversion;
""")

# DATA QUALITY CHECK QUERIES

null_check_songplays = """
    SELECT COUNT(*) 
    FROM songplays_fact 
    WHERE user_id IS NULL OR song_id IS NULL OR artist_id IS NULL OR start_time IS NULL;
"""

year_consistency_check = """
    SELECT COUNT(*) 
    FROM songs_dim 
    WHERE year < 1900 OR year > EXTRACT(YEAR FROM CURRENT_DATE);
"""

start_time_accuracy_check = """
    SELECT COUNT(*) 
    FROM songplays_fact sp
    JOIN staging_events se ON sp.start_time = TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'
    WHERE sp.start_time IS NULL;
"""

unique_check_songs = """
    SELECT song_id, COUNT(*) 
    FROM songs_dim 
    GROUP BY song_id 
    HAVING COUNT(*) > 1;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
data_quality_queries = [null_check_songplays, year_consistency_check, start_time_accuracy_check, unique_check_songs]