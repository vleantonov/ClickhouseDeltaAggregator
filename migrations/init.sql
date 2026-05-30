
CREATE TABLE IF NOT EXISTS transactions (
    date Date,
    transaction_id String,
    user_id String,
    product_id String,
    amount Int64,

    partition_id UInt64,
    offset UInt64,
    write_time DateTime DEFAULT NOW()
) ENGINE = ReplicatedMergeTree()
ORDER BY (partition_id, date, user_id, offset);

CREATE TABLE IF NOT EXISTS transactions_d 
AS transactions
ENGINE = Distributed('{cluster}', currentDatabase(), 'transactions', xxHash64(partition_id));

CREATE TABLE IF NOT EXISTS reports (
    date Date,
    user_id String,
    product_id String,
    
    partition_id UInt64,
    amount SimpleAggregateFunction(sum, Int64),   
    offset SimpleAggregateFunction(max, UInt64),
    write_time SimpleAggregateFunction(max, DateTime)
) ENGINE = ReplicatedAggregatingMergeTree()
ORDER BY (date, user_id, product_id);

CREATE TABLE IF NOT EXISTS reports_d 
AS reports
ENGINE = Distributed('{cluster}', currentDatabase(), 'reports', xxHash64(partition_id));

CREATE MATERIALIZED VIEW IF NOT EXISTS reports_mv TO reports
AS SELECT 
    date,
    user_id,
    product_id,
    partition_id,
    SUM(amount) AS amount,
    MAX(offset) AS offset,
    MAX(write_time) AS write_time
FROM transactions
GROUP BY ALL;
