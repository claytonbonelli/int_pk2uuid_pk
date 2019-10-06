# int_pk2uuid_pk
Change primary keys integer/bigint to UUID

###### By Clayton Boneli


# UNDER CONSTRUTION!!!!!!

Some database the primary keys and related foreign keys are defined as numeric sequentials (integers), however in the world of cloud applications, the use of integers is not the default. The work of changing primary / foreign keys from whole numeric columns to UUID columns is made easier by using a tool that does these transformations. The goal of this project is to create a tool that transforms primary / foreign keys to UUID columns.

# Limitations:
* Postgres database
* Only integer / bigint keys
* compound keys are not accepted

# ATENCION: before use this tool, do a database backup
