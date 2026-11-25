CREATE TABLE daily_metrics (
    date date NOT NULL,
    metric text NOT NULL,
    value double precision NOT NULL,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (date, metric)
);

CREATE TABLE top_movies (
    date date NOT NULL,
    rank smallint NOT NULL CHECK (rank > 0),
    movie_id text NOT NULL,
    views bigint NOT NULL,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (date, rank)
);
