-- https://stackoverflow.com/questions/25145648/select-from-large-table-or-split-table-and-make-many-small-queries


-------------------------------------------------------------
--  TABLE initial_table
--DROP TABLE segments CASCADE;
CREATE TABLE initial_table (
  user_id NUMERIC(38,0) NOT NULL,
  session_id NUMERIC(38,0) NOT NULL,
  segment_id NUMERIC(38,0) NOT NULL,
  segment_description VARCHAR(100) NOT NULL,
  user_city VARCHAR(50) NOT NULL,
  server_time TIMESTAMP NOT NULL,
  device_browser VARCHAR (10) NOT NULL,
  device_os VARCHAR (5) NOT NULL,
  device_mobile VARCHAR (10) NOT NULL,
  time_spent NUMERIC(38,0) NOT NULL,
  event_id NUMERIC(38,0) ,
  event_description VARCHAR(50) NOT NULL,
  crash_detection VARCHAR(100) NOT NULL
);

-- Algunos valores de ejemplo
INSERT INTO initial_table(user_id, session_id, segment_id, segment_description, user_city, server_time, device_browser, device_os, device_mobile, time_spent, event_id, event_description, crash_detection) 
VALUES (1, 1, 1, 'gold', 'cordoba', '2020-03-01 00:00:00', 'chrome', 'win', 'no', 5, 1, 'description_1', 'no');

INSERT INTO initial_table(user_id, session_id, segment_id, segment_description, user_city, server_time, device_browser, device_os, device_mobile, time_spent, event_id, event_description, crash_detection) 
VALUES (2, 2, 2, 'black', 'chubut', '2020-05-19 00:00:00', 'firefox', 'win', 'no', 7, 3, 'description_3', 'no');

INSERT INTO initial_table(user_id, session_id, segment_id, segment_description, user_city, server_time, device_browser, device_os, device_mobile, time_spent, event_id, event_description, crash_detection) 
VALUES (3, 3, 2, 'black', 'salta', '2020-10-06 00:00:00', 'opera', 'win', 'no', 8, 6, 'description_6', 'no');

INSERT INTO initial_table(user_id, session_id, segment_id, segment_description, user_city, server_time, device_browser, device_os, device_mobile, time_spent, event_id, event_description, crash_detection) 
VALUES (4, 4, 1, 'gold', 'chaco', '2020-12-09 00:00:00', 'chrome', 'win', 'no', 10, 1, 'description_1', 'no');


-------------------------------------------------------------
-- TABLE segments
-- DROP TABLE segments CASCADE;
-- Asumo que cada segment_id tiene una y sola una segment_description
CREATE TABLE segments AS SELECT 
  DISTINCT segment_id, segment_description
  FROM initial_table
;

ALTER TABLE segments
ADD CONSTRAINT PK_segments PRIMARY KEY(segment_id);


-------------------------------------------------------------
-- TABLE users
-- DROP TABLE users CASCADE;
-- Asumo que cada user_id corresponde a un solo segment_id
CREATE TABLE users AS SELECT 
  DISTINCT user_id, segment_id
  FROM initial_table
;

ALTER TABLE users
ADD CONSTRAINT PK_users PRIMARY KEY(user_id),
ADD CONSTRAINT FK_segment_user FOREIGN KEY (segment_id) REFERENCES segments (segment_id);


-------------------------------------------------------------
-- TABLE sessions
-- DROP TABLE sessions CASCADE;
-- Asumo que cada session_id puede ser iniciada desde solo una user_city, puede tener
-- un solo time_spent, un solo valor de crash_detection y puede ser iniciad por un solo user_id
CREATE TABLE sessions AS SELECT
  DISTINCT session_id, user_city, time_spent, crash_detection, user_id
  FROM initial_table
;


ALTER TABLE sessions
ADD CONSTRAINT PK_sessions PRIMARY KEY(session_id),
ADD CONSTRAINT FK_user_session FOREIGN KEY (user_id) REFERENCES users (user_id);


-------------------------------------------------------------
-- TABLE events
-- DROP TABLE events CASCADE;
-- Primero creo una table con los DISTINCT event_id
CREATE TABLE events AS SELECT
  DISTINCT event_id
  FROM initial_table
;
-- PARTITION BY RANGE (event_date);


-- Agrego otras columnas
ALTER TABLE events
ADD COLUMN event_description VARCHAR(50), 
ADD COLUMN device_browser VARCHAR (10), 
ADD COLUMN device_os VARCHAR (5), 
ADD COLUMN device_mobile VARCHAR (10), 
ADD COLUMN server_time TIMESTAMP,
ADD COLUMN user_id NUMERIC(38,0), 
ADD COLUMN session_id NUMERIC(38,0);


-- Actualizo el valor de las columnas agregadas de acuerdo al valor que tienen en initial_table, por event_id
UPDATE events AS e 
SET event_description = it.event_description,
    device_browser = it.device_browser,
    device_os = it.device_os,
    device_mobile = it.device_mobile,
    server_time = it.server_time,
    user_id = it.user_id,
    session_id = it.session_id
FROM initial_table AS it
WHERE e.event_id = it.event_id;


ALTER TABLE events
ADD CONSTRAINT PK_events PRIMARY KEY(event_id),
ADD CONSTRAINT FK_user_events FOREIGN KEY (user_id) REFERENCES users (user_id),
ADD CONSTRAINT FK_session_events FOREIGN KEY (session_id) REFERENCES sessions (session_id);


-------------------------------------------------------------
-- TABLE hb_first
-- DROP TABLE hb_first CASCADE;
CREATE TABLE hb_first AS SELECT
  user_id,
  event_id
  FROM initial_table
  WHERE event_id = 1
;

ALTER TABLE hb_first
ADD CONSTRAINT FK_user_hb_first FOREIGN KEY (user_id) REFERENCES users (user_id),
ADD CONSTRAINT FK_events_hb_first FOREIGN KEY (event_id) REFERENCES events (event_id);


-------------------------------------------------------------
-- TABLE daily_activity 
-- DROP TABLE daily_activity CASCADE;
CREATE TABLE daily_activity AS SELECT 
  user_id,
  event_id,
  session_id
  FROM initial_table
;

ALTER TABLE daily_activity
ADD CONSTRAINT FK_user_daily_activity FOREIGN KEY (user_id) REFERENCES users (user_id),
ADD CONSTRAINT FK_events_daily_activity FOREIGN KEY (event_id) REFERENCES events (event_id),
ADD CONSTRAINT FK_sessions_daily_activity FOREIGN KEY (session_id) REFERENCES sessions (session_id);


-------------------------------------------------------------
-- DROP tables
DROP TABLE initial_table;
DROP TABLE segments CASCADE;
DROP TABLE users CASCADE;
DROP TABLE sessions CASCADE;
DROP TABLE events CASCADE;
DROP TABLE hb_first CASCADE;
DROP TABLE daily_activity CASCADE;