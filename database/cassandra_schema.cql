CREATE KEYSPACE IF NOT EXISTS health_insight
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE health_insight;

CREATE TABLE IF NOT EXISTS patient_vitals (
patient_id text,
timestamp timestamp,
metric_name text,
value double,
unit text,
PRIMARY KEY ((patient_id),timestamp, metric_name)
) WITH CLUSTERING ORDER BY (timestamp DESC, metric_name ASC);


CREATE TABLE IF NOT EXISTS health_alerts (
alert_id uuid PRIMARY KEY,
patient_id text,
metric_name text,
observed_value double,
alert_type text, 
timestamp timestamp,
status text      
);

CREATE TABLE IF NOT EXISTS health_alerts_by_day (
day date,
timestamp timestamp,
alert_id uuid,
patient_id text,
metric_name text,
observed_value double,
alert_type text,
status text,
PRIMARY KEY ((day), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, alert_id ASC);


CREATE TABLE IF NOT EXISTS health_alerts_by_patient (
patient_id text,
timestamp timestamp,
alert_id uuid,
metric_name text,
observed_value double,
alert_type text,
status text,
day date,
PRIMARY KEY ((patient_id), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, alert_id ASC);


CREATE TABLE IF NOT EXISTS daily_health_summary (
stat_date date,
metric_name text,
avg_value double,
min_value double,
max_value double,
patient_count int,
PRIMARY KEY (stat_date, metric_name)
);