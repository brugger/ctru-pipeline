DROP DATABASE IF EXISTS ctru_tracker;
CREATE DATABASE ctru_tracker;
USE ctru_tracker;


CREATE TABLE status_tracking (

  run_name            VARCHAR(80) NOT NULL,
  thread_name         VARCHAR(80) NOT NULL,

  step                VARCHAR(80) NOT NULL,
  step_nr	      INT DEFAULT 0,
  run_time	      varchar(80),
  max_mem	      varchar(80),
  done		      INT DEFAULT 0,
  running 	      INT DEFAULT 0,
  queuing	      INT DEFAULT 0,
  failed	      INT DEFAULT 0,
  unknown	      INT DEFAULT 0,
  time  	      timestamp,


  PRIMARY  KEY ref_idx  (run_name, thread_name, step),
  KEY rname_idx (run_name),
  KEY tname_idx (thread_name)

) ENGINE INNODB;


CREATE TABLE progress_tracking (

  run_name            VARCHAR(80) NOT NULL, 
  thread_name         VARCHAR(80) NOT NULL,

  steps_done          INT NOT NULL,
  steps_total         INT NOT NULL,
  time  	      timestamp,

  PRIMARY  KEY ref_idx  (run_name, thread_name),
  KEY rname_idx (run_name),
  KEY tname_idx (thread_name)


) ENGINE INNODB;



