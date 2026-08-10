CREATE TABLE IF NOT EXISTS movies(
    imdb INTEGER NOT NULL,
    server VARCHAR(32) NOT NULL,
    segments_count INTEGER NOT NULL,
    duration FLOAT NOT NULL,
    last_acces INTEGER NOT NULL,
    PRIMARY KEY(imdb, server)
);

CREATE TABLE IF NOT EXISTS segments(
    imdb INTEGER NOT NULL,
    server VARCHAR(32) NOT NULL,
    segment INTEGER NOT NULL,
    last_acces INTEGER,
    size INTEGER NOT NULL,
    start_time FLOAT,
    PRIMARY KEY(imdb, server, segment)
);

CREATE TABLE IF NOT EXISTS subtitles(
    imdb INTEGER NOT NULL,
    server VARCHAR(32) NOT NULL,
    language VARCHAR(4) NOT NULL,
    text VARCHAR NOT NULL,
    PRIMARY KEY(imdb, server, language)
);

CREATE INDEX IF NOT EXISTS idx_movies_last_acces ON movies(last_acces) WHERE last_acces IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_segments_last_acces ON segments(imdb, server, last_acces, size, segment, start_time) WHERE last_acces IS NOT NULL;