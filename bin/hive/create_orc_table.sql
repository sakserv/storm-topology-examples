CREATE TABLE IF NOT EXISTS default.test (id INT, msg STRING)
PARTITIONED BY (dt STRING)
CLUSTERED BY (id) INTO 16 BUCKETS
STORED AS ORC tblproperties("orc.compress"="NONE")