CREATE UNLOGGED TABLE benchmark_data (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    value      DOUBLE PRECISION,
    amount     INTEGER,
    category   TEXT,
    created_at TIMESTAMP
);

-- Insert 1 billion rows in batches of 10 million
DO $$
DECLARE
    batch_size INT := 10000000;
    total_rows INT := 20000000;
    batch_start INT := 1;
BEGIN
    WHILE batch_start <= total_rows LOOP
        INSERT INTO benchmark_data (id, name, value, amount, category, created_at)
        SELECT
            g AS id,
            md5(g::text) AS name,
            random() * 10000 AS value,
            (random() * 1000)::int AS amount,
            'cat_' || (g % 20) AS category,
            timestamp '2020-01-01' + (random() * 1500)::int * interval '1 day'
        FROM generate_series(batch_start, LEAST(batch_start + batch_size - 1, total_rows)) g;
        RAISE NOTICE 'Inserted rows % to %', batch_start, LEAST(batch_start + batch_size - 1, total_rows);
        batch_start := batch_start + batch_size;
    END LOOP;
END $$;

ANALYZE benchmark_data;
