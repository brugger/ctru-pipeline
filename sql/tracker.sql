DROP DATABASE IF EXISTS gemini_tracker;
CREATE DATABASE gemini_tracker;
USE gemini_tracker;


CREATE TABLE status_tracking (

  name                VARCHAR(80) NOT NULL,
  step                VARCHAR(80) NOT NULL,
  status              VARCHAR(80) NOT NULL,
  count		      INT NOT NULL,
  time  	      timestamp,


  PRIMARY  KEY ref_idx (name, step, status)

) ENGINE INNODB;
