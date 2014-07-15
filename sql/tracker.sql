DROP DATABASE IF EXISTS ctru_tracker;
CREATE DATABASE ctru_tracker;
USE ctru_tracker;


CREATE TABLE status_tracking (

  run_name            VARCHAR(80) NOT NULL,
  thread_name         VARCHAR(80) NOT NULL,

  step                VARCHAR(80) NOT NULL,
  step_nr	      INT DEFAULT 0,
  done		      INT DEFAULT 0,
  running 	      INT DEFAULT 0,
  queuing	      INT DEFAULT 0,
  failed	      INT DEFAULT 0,
  unknown	      INT DEFAULT 0,
  time  	      timestamp,


  PRIMARY  KEY ref_idx  (run_name, step),
  KEY ref2_idx (thread_name, step)

) ENGINE INNODB;


CREATE TABLE progress_tracking (

  run_name            VARCHAR(80) NOT NULL PRIMARY  KEY, 
  thread_name         VARCHAR(80) NOT NULL,

  steps_done          INT NOT NULL,
  steps_total         INT NOT NULL,
  time  	      timestamp

) ENGINE INNODB;



