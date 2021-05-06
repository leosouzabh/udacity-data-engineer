# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"


# CREATE TABLES

songplay_table_create = ("""
create table songplays(
    songplay_id serial primary key,
    start_time bigint references time not null,
    user_id int references users not null,
    level varchar not null,
    song_id varchar references songs,
    artist_id varchar references artists,
    session_id int not null,
    location varchar not null,
    user_agent varchar not null
)
""")

user_table_create = ("""
create table users (
    user_id int primary key,
    first_name varchar not null,
    last_name varchar not null,
    gender varchar not null,
    level varchar not null,
    last_activity_ts bigint not null
)
""")

song_table_create = ("""
create table songs (
    song_id varchar primary key,
    title varchar not null,
    artist_id varchar not null,
    year int not null,
    duration int not null
)
""")

artist_table_create = ("""
create table artists (
    artist_id varchar primary key,
    name varchar not null,
    location varchar not null,
    latitude varchar not null,
    longitude varchar not null
)
""")

time_table_create = ("""
create table time(
    start_time bigint primary key,
    hour int not null,
    day int not null,
    week int not null,
    month int not null,
    year int not null,
    weekday int not null
)
""")


# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s
)
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level, last_activity_ts)
values (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
     DO UPDATE SET level = (
         CASE
             WHEN EXCLUDED.last_activity_ts > users.last_activity_ts 
             THEN EXCLUDED.level ELSE users.level
         END)
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
    values (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) 
        DO NOTHING
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
    values (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) 
        DO NOTHING
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
    values (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) 
        DO NOTHING
""")


# FIND SONGS

song_select = ("""
SELECT a.artist_id, s.song_id
  FROM songs as s
       JOIN artists as a
         ON a.artist_id = s.artist_id
 WHERE a.name = %s 
   AND s.title = %s 
   AND s.duration = %s 
 LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]