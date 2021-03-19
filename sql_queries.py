import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stage_events(
    artist			text
    , auth			text
    , firstName		text
    , gender		text
    , iteminSession	int
    , lastName		text
    , length		text
    , level			text
    , location		text
    , method		text
    , page			text
    , registration	text
    , sessionId		int
    , song			text
    , status		int
    , ts			bigint
    , userAgent		text
    , userId		int

)
""")

staging_songs_table_create = ("""
CREATE TABLE stage_songs(
    num_songs			int,
    artist_id			text,
    artist_latitude		numeric(10,5),
    artist_longitude	numeric(10,5),
    artist_location		text,
    artist_name			text,
    song_id				text,
    title				text,
    duration			float,
    year				int
)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id int IDENTITY(0,1) CONSTRAINT PK_songplays PRIMARY KEY sortkey distkey
, start_time timestamp NOT NULL REFERENCES time(start_time)
, user_id int NOT NULL REFERENCES users(user_id)
, level text
, song_id text NOT NULL REFERENCES songs(song_id)
, artist_id text NOT NULL REFERENCES artists(artist_id)
, session_id int
, location text
, user_agent text
,CONSTRAINT UQ_start_time_user_id_song_id_artist_id UNIQUE(start_time,user_id,song_id,artist_id)
    )
""")

user_table_create = ("""
CREATE TABLE users(
	user_id int CONSTRAINT PK_users PRIMARY KEY sortkey
	, first_name text
	, last_name text
	, gender text
	, level text
	)
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs(
	song_id text CONSTRAINT PK_songs PRIMARY KEY sortkey
	, title text
	, artist_id text
	, year int
	, duration numeric(20,10)
	)
    diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists(
	artist_id text CONSTRAINT PK_artists PRIMARY KEY sortkey
	, name text
	, location text
	, latitude numeric(10,5)
	, longitude numeric(10,5)
	)
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE time(
	start_time timestamp CONSTRAINT PK_time PRIMARY KEY sortkey
	, hour int
    , day int
	, week int
    , month int
	, year int
	, weekday int
	)
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stage_events
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT AS JSON {}
TIMEFORMAT 'epochmillisecs';
""").format(config['S3']['LOG_DATA'],
config['IAM_ROLE']['ARN'],
config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY stage_songs
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'],
config['IAM_ROLE']['ARN'])




# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT timestamp 'epoch' + A.ts/1000 * interval '1 second', CAST(A.userId as INT)
    , A.level, B.song_id, B.artist_id, CAST(A.sessionId as INT), A.location, A.userAgent 
FROM stage_events A
JOIN stage_songs B ON A.artist = B.artist_name 
    AND A.length = B.duration 
    AND A.song = B.title
WHERE A.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT DISTINCT CAST(userId as INT), firstName, lastName, gender, level
FROM stage_events 
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id, title, artist_id, CAST(year as INT), CAST(duration as numeric(20,10))
FROM stage_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude , longitude) 
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM stage_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT start_time, extract(hour from start_time), extract(day from start_time)
    , extract(week from start_time), extract(month from start_time)
    , extract(year from start_time), extract(weekday from start_time)
FROM songplays
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
