# int_pk2uuid_pk
Convert primary keys integer or bigint to UUID

###### By Clayton Boneli

# ATENCION: backup your data before use this software

Some database the primary keys and related foreign keys are defined as numeric sequentials (integers), however in the world of cloud applications, the use of integers is not the default. The work of changing primary / foreign keys from whole numeric column to UUID column is made easier by using a tool that does these transformations. The goal of this project is to create a tool that transforms primary / foreign keys to UUID columns.

# Limitations:
* Postgres database
* Only integer / bigint keys
* compound keys are not accepted

# How to use:
* Backup your data
* Use inheritance to add any specific behavior
* Use the set_up method:
    * to delete trigers, views and other objects that depend of the columns to be cast 
    * to delete any check constraints for all columns that will be cast
    
* Use the tear_down method to restore any changes made during the set_up method 

```python
from replace_id import IdReplacer


class MyReplacer(IdReplacer):
    def set_up(self, connection, *args, **kwargs):
        utils = kwargs['utils']
        sql = "create extension if not exists pgcrypto;"
        utils.execute(connection, sql)


MyReplacer().execute(
    params={
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'postgres',
        'schema': 'public',
        'db_name': 'clayton',
        'serial_name': 'serial_id',
        'autocommit': True,
    }
)
```

# Actions in the database

The following actions are performed on each of the tables that will have their PK / FK columns changed from integer / bigint to UUID:
* Creation of a temporary column to store the new UUID value
* Creation of a column ("serial_id") that will receive a copy of the current PK value. This column will be kept in the table if references to the old PK value are required.
* Exclusion of all  FKs constraints
* All FK columns will be changed from int / bigint to varchar datatype
* Copy UUID values from related tables to the FK columns
* Change FK columns  from varchar to UUID datatype
* Change PK datatype to UUID
* Add a default value to pk (to generate, automatically, the uuid)
* Copy UUID values from temporary columns to PK column
* Creation of FK constraints
* Drop of the temporary column
