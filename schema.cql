
CREATE KEYSPACE test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE test;

CREATE TABLE test (
  activity_id timeuuid,
  PRIMARY KEY (activity_id)
);

COPY test (activity_id) FROM 'test_data.csv';
