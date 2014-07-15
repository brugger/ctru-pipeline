DROP DATABASE IF EXISTS ctru_tracker;
CREATE DATABASE ctru_tracker;
USE ctru_tracker;


CREATE TABLE status_tracking (

  run_name            VARCHAR(80) NOT NULL,
  thread_name         VARCHAR(80) NOT NULL,

  step                VARCHAR(80) NOT NULL,
  done		      INT,
  running, 	      INT,
  queuing	      INT,
  failed	      INT,
  unknown	      INT,
  time  	      timestamp,


  PRIMARY  KEY ref_idx  (run_name, step),
  PRIMARY  KEY ref2_idx (thread_name, step)

) ENGINE INNODB;


CREATE TABLE progress_tracking (

  run_name            VARCHAR(80) NOT NULL PRIMARY  KEY, 
  thread_name         VARCHAR(80) NOT NULL,

  steps_done          INT NOT NULL,
  steps_total         INT NOT NULL,
  time  	      timestamp

) ENGINE INNODB;



