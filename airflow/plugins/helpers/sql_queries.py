class SqlQueries:
    create_events_table = ("""
        CREATE TABLE IF NOT EXISTS staging_events(\
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender varchar,
                                itemInSession int,
                                lastName varchar,
                                length float,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration float,
                                sessionId int,
                                song varchar,
                                status int,
                                ts bigint,
                                userAgent varchar,
                                userId int
            )
        
    """)
    
    create_songs_table = ("""
       CREATE TABLE IF NOT EXISTS staging_songs(\
                                num_songs int,
                                artist_id varchar,
                                artist_latitude float,
                                artist_longitude float,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration float,
                                year int

            ) 
    """)
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        AND userID IS NOT NULL
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
    
    create_table_songplays = ("""
        CREATE TABLE IF NOT EXISTS songplays(\
                        songplay_id varchar PRIMARY KEY, \
                        start_time timestamp NOT NULL, \
                        user_id int NOT NULL,\
                        level varchar,\
                        song_id varchar,\
                        artist_id varchar,\
                        session_id int,\
                        location varchar,\
                        user_agent varchar);
    
    """)
    
    create_table_users = ("""
        CREATE TABLE IF NOT EXISTS users(
                    user_id int PRIMARY KEY,
                    first_name varchar,
                    last_name varchar,
                    gender varchar,
                    level varchar)
    """)
    
    create_table_songs = ("""
        CREATE TABLE IF NOT EXISTS songs(
                    song_id varchar PRIMARY KEY,
                    title varchar,
                    artist_id varchar NOT NULL,
                    year int,
                    duration float)
        
    """)
    create_table_artists = ("""
        CREATE TABLE IF NOT EXISTS artists(
                        artist_id varchar PRIMARY KEY,
                        name varchar,
                        location varchar,
                        latitude float,
                        longitude float)
    """)
    
    create_table_time = ("""
        CREATE TABLE IF NOT EXISTS time (
                    start_time timestamp PRIMARY KEY,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday int)
    """)
    
    drop_all_tables = ("""
        DROP TABLE IF EXISTS staging_events, staging_songs, songs, users, artists, time, songplays;
    """)
    