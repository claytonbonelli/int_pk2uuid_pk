# int_pk2uuid_pk
Change primary keys integer/bigint to UUID

###### By Clayton Boneli

# ATENCION: backup your data before use this tool

Some database the primary keys and related foreign keys are defined as numeric sequentials (integers), however in the world of cloud applications, the use of integers is not the default. The work of changing primary / foreign keys from whole numeric columns to UUID columns is made easier by using a tool that does these transformations. The goal of this project is to create a tool that transforms primary / foreign keys to UUID columns.

# Limitations:
* Postgres database
* Only integer / bigint keys
* compound keys are not accepted
* Under constrution

# How to use:
* Backup your data
* Use inheritance to add any specific behavior
* Use the set_up method:
    ** to delete the default values for any column that will be cast to uuid
    ** to delete trigers, views and other objects that depend of the columns to be cast 
    ** to delete any check constraints for all columns that will be cast
* Use the tear_down method to restore any changes made during the set_up method 

