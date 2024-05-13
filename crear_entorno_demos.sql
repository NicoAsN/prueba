use role accountadmin;
use warehouse compute_wh;

create or replace database demosdb;
create or replace schema raw_sch;
create or replace schema refined_sch;
create or replace schema shared_sch;

drop schema demosdb.public;

use database demosdb;
use schema demosdb.raw_sch;