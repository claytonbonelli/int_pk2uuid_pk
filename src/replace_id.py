import psycopg2
from psycopg2.extras import RealDictCursor


class DatabaseUtils:
    """
    Some utils to manage database conections, selects, etc.
    """

    def get_connection(self, params):
        """
        OPen a connection to Postgres database.
        :param params: a dict with connection parameters: host, user, password, schema and database
        :return: the opened connection
        """
        conn_string = "host='{host}' dbname='{db_name}' user='{user}' password='{password}' options='-c search_path={schema},public'"

        conn_string = conn_string.format(
            host=params["host"],
            user=params["user"],
            password=params["password"],
            schema=params["schema"],
            db_name=params["db_name"],
        )
        connection = psycopg2.connect(conn_string)
        connection.autocommit = params['autocommit']
        return connection

    def select(self, connection, select_command):
        """
        Perform a select command.
        :param connection: a opened connection.
        :param select_command: the select command
        :return: the rows returned by the select command.
        """
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(select_command)
        rows = cursor.fetchall()
        return rows

    def execute(self, connection, sql_command):
        """
        Perform a SQL command.
        :param connection: a opened connection.
        :param select_command: the select command
        """
        cursor = connection.cursor()
        cursor.execute(sql_command)


class IdReplacer:
    """
    Perform the ID replace.
    """

    def __init__(self):
        self.primary_keys = None
        self.foreign_keys = None

    def execute(self, *args, **kwargs):
        """
        Replace the sequencial Id by the UUID.
        :param args: some arguments.
        :param kwargs: some key-arguments.

        """
        params = kwargs.get('params')
        if params is None:
            raise Exception('Params not defined')
        utils = DatabaseUtils()
        kwargs['utils'] = utils
        conn = utils.get_connection(params)
        try:
            with conn:
                # Primary Key
                self.primary_keys = self._get_primary_keys(conn)
                self.foreign_keys = self._get_foreign_keys(conn)
                self._set_up(conn, *args, **kwargs, rows=self.primary_keys)
                try:
                    print("Getting primary keys")
                    kwargs['rows'] = self.primary_keys
                    print("Adding temporary column")
                    self._add_temporary_column(conn, *args, **kwargs)
                    print("Assign values to temporary column")
                    self._assign_value_to_temporary_pk_column(conn, *args, **kwargs)
                    print("Adding serial column")
                    self._add_serial_column(conn, *args, **kwargs)
                    print("Copying pk to serial column")
                    self._copy_pk_column_to_serial_column(conn, *args, **kwargs)
                    # Foreign Key
                    print("Getting foreign keys")
                    kwargs['rows'] = self.foreign_keys
                    print("Dropping fk constraints")
                    self._drop_fk_constraint(conn, *args, **kwargs)
                    print("Changing fk to varchar")
                    self._change_fk_column_to_datatype(conn, *args, **kwargs, data_type='varchar')
                    print("Copying fk values")
                    self._copy_pk_values_to_fk_columns(conn, *args, **kwargs)
                    print("Changing fk to uuid")
                    self._change_fk_column_to_datatype(conn, *args, **kwargs, data_type='uuid')
                    print("Changing pk to uuid")
                    kwargs['rows'] = self.primary_keys
                    self._change_pk_column_to_uuid(conn, *args, **kwargs)
                    print("Setting the uuid primary key value")
                    self._copy_temporary_column_to_pk(conn, *args, **kwargs)
                    print("Recreating fk constraints")
                    kwargs['rows'] = self.foreign_keys
                    self._create_fk_constraint(conn, *args, **kwargs)
                finally:
                    self._tear_down(conn, *args, **kwargs)
        finally:
            conn.close()

    def _set_up(self, connection, *args, **kwargs):
        pass

    def _tear_down(self, connection, *args, **kwargs):
        pass

    def _build_sql_to_drop_default_value(self, table_name, column_name):
        sql = "alter table if exists {table_name} alter column {column_name} drop default".format(
            table_name=table_name,
            column_name=column_name,
        )
        return sql

    def _drop_column_default_value(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']
        for row in rows:
            table_name = self._build_table_name(row['table_schema'], row['table_name'])
            column_name = row['column_name']
            sql = self._build_sql_to_drop_default_value(table_name, column_name)
            utils.execute(connection, sql)

    def _build_sql_to_copy_pk_values_to_fk_columns(
            self, table_name, column_name, temp_name, foreign_table_name, foreign_column_name
    ):
        sql = """
        update {table_name} a 
        set {column_name} = x.{temp_name}::varchar
        from {foreign_table_name} x
        where a.{column_name}::varchar = x.{foreign_column_name}::varchar;
        """.format(
            table_name=table_name,
            temp_name=temp_name,
            foreign_table_name=foreign_table_name,
            column_name=column_name,
            foreign_column_name=foreign_column_name,
        )
        return sql

    def _copy_pk_values_to_fk_columns(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']

        for row in rows:
            table_schema = row['table_schema']
            table_name = row['table_name']
            table_name = self._build_table_name(table_schema, table_name)

            column_name = row['column_name']
            foreign_table_name = self._build_table_name(row['foreign_table_schema'], row['foreign_table_name'])
            foreign_column_name = row['foreign_column_name']
            temp_name = self._build_temp_column_name(foreign_column_name)

            sql = self._build_sql_to_copy_pk_values_to_fk_columns(
                table_name, column_name, temp_name, foreign_table_name, foreign_column_name
            )
            if sql is not None:
                utils.execute(connection, sql)

    def _build_sql_to_alter_column_datatype(self, table_name, column_name, data_type):
        sql = """
        alter table {table_name} alter column {column_name} type {data_type} using {column_name}::{data_type};
        """.format(
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
        )
        return sql

    def _build_sql_to_alter_pk_column_to_uuid(self, table_name, column_name):
        sql = """
        alter table {table_name} alter column {column_name} type uuid 
        using cast(lpad(to_hex({column_name}), 32, '0') as uuid);
        """.format(
            table_name=table_name,
            column_name=column_name,
        )
        return sql

    def _change_fk_column_to_datatype(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        data_type = kwargs['data_type']
        utils = kwargs['utils']
        for row in rows:
            column_data_type = row['data_type']
            if column_data_type == data_type:
                continue

            schema_name = row['table_schema']
            table_name = row['table_name']
            table_name = self._build_table_name(schema_name, table_name)
            column_name = row['column_name']

            sql = self._build_sql_to_alter_column_datatype(table_name, column_name, data_type)
            if sql is not None:
                utils.execute(connection, sql)

    def _change_pk_column_to_uuid(self, connection, *args, **kwargs):
        rows = self._get_primary_keys(connection)
        utils = kwargs['utils']
        for row in rows:
            data_type = row['data_type']
            if data_type == 'uuid':
                continue

            schema_name = row['table_schema']
            table_name = row['table_name']
            table_name = self._build_table_name(schema_name, table_name)
            column_name = row['column_name']

            sql = self._build_sql_to_alter_pk_column_to_uuid(table_name, column_name)
            if sql is not None:
                utils.execute(connection, sql)

    def _build_sql_to_drop_constraint(self, table_name, constraint_name):
        sql = "alter table {table_name} drop constraint if exists {constraint_name};".format(
            table_name=table_name,
            constraint_name=constraint_name,
        )
        return sql

    def _build_sql_to_create_constraint(
            self, table_name, constraint_name, column_name, foreign_table_name, foreign_column_name
    ):
        sql = """
        alter table {table_name} add constraint {constraint_name} 
        foreign key ({column_name}) references {foreign_table_name} ({foreign_column_name}); 
        """.format(
            table_name=table_name,
            constraint_name=constraint_name,
            column_name=column_name,
            foreign_table_name=foreign_table_name,
            foreign_column_name=foreign_column_name,
        )
        return sql

    def _drop_fk_constraint(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']
        for row in rows:
            schema_name = row['table_schema']
            table_name = row['table_name']
            table_name = self._build_table_name(schema_name, table_name)
            constraint_name = row['constraint_name']

            sql = self._build_sql_to_drop_constraint(table_name, constraint_name)
            if sql is not None:
                utils.execute(connection, sql)

    def _create_fk_constraint(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']
        for row in rows:
            table_name = self._build_table_name(row['table_schema'], row['table_name'])
            constraint_name = row['constraint_name']
            column_name = row['column_name']
            foreign_table_name = self._build_table_name(row['foreign_table_schema'], row['foreign_table_name'])
            foreign_column_name = row['foreign_column_name']

            sql = self._build_sql_to_create_constraint(
                table_name, constraint_name, column_name, foreign_table_name, foreign_column_name
            )
            if sql is not None:
                utils.execute(connection, sql)

    def _build_sql_to_update_column(self, table_name, column_name, value):
        sql = "update {table_name} set {column_name} = {value};".format(
            table_name=table_name,
            column_name=column_name,
            value=value,
        )
        return sql

    def _copy_pk_column_to_serial_column(self, connection, *args, **kwargs):
        serial_name = kwargs['params']['serial_name']
        rows = kwargs['rows']
        utils = kwargs['utils']

        for row in rows:
            table_schema = row['table_schema']
            table_name = self._build_table_name(table_schema, row['table_name'])
            column_name = row['column_name']

            sql = self._build_sql_to_update_column(table_name, serial_name, column_name)
            if sql is not None:
                utils.execute(connection, sql)

    def _build_primary_key_update_command(self, *args, **kwargs):
        return kwargs['update_command'].format(value='gen_random_uuid()')

    def _copy_temporary_column_to_pk(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']

        for row in rows:
            table_name = self._build_table_name(row['table_schema'], row['table_name'])
            column_name = row['column_name']
            temp_column = self._build_temp_column_name(column_name)

            sql = self._build_sql_to_update_column(table_name, column_name, temp_column)
            if sql is not None:
                utils.execute(connection, sql)

    def _assign_value_to_temporary_pk_column(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']

        for row in rows:
            data_type = row['data_type']
            table_schema = row['table_schema']
            table_name = self._build_table_name(table_schema, row['table_name'])
            column_name = self._build_temp_column_name(row['column_name'])

            update_command = self._build_sql_to_update_column(table_name, column_name, '{value}')
            update_command = self._build_primary_key_update_command(
                *args,
                **kwargs,
                connection=connection,
                table_schema=table_schema,
                table_name=table_name,
                column_name=column_name,
                data_type=data_type,
                update_command=update_command,
            )
            if update_command is not None:
                utils.execute(connection, update_command)

    def _get_primary_keys(self, connection):
        sql = """
        select distinct tc.table_schema, tc.table_name, kcu.column_name, c.data_type
        from information_schema.table_constraints as tc
        inner join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name
        inner join information_schema.columns c on tc.table_schema = c.table_schema and tc.table_name = c.table_name and kcu.column_name = c.column_name
        where constraint_type = 'PRIMARY KEY'
        and data_type in ('integer', 'bigint')
        """
        rows = DatabaseUtils().select(connection, sql)
        return rows

    def _get_foreign_keys(self, connection):
        sql = """
        select distinct tc.table_schema, tc.table_name, kcu.column_name, c.data_type, ccu.table_schema as foreign_table_schema, 
          ccu.table_name as foreign_table_name, ccu.column_name as foreign_column_name, tc.constraint_name
        from information_schema.table_constraints as tc
        inner join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name
        inner join information_schema.constraint_column_usage ccu on ccu.constraint_name = tc.constraint_name
        inner join information_schema.columns c on tc.table_schema = c.table_schema and tc.table_name = c.table_name and kcu.column_name = c.column_name
        where constraint_type = 'FOREIGN KEY'
        and data_type in ('integer', 'bigint')
        """
        rows = DatabaseUtils().select(connection, sql)
        return rows

    def _build_temp_column_name(self, column_name):
        return '%s_temp2replace' % column_name

    def _build_table_name(self, schema_name, table_name):
        return '%s.%s' % (schema_name, table_name)

    def _build_sql_to_add_column(self, table_name, column_name, data_type):
        sql = "alter table {table_name} add column if not exists {column_name} {data_type};".format(
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
        )
        return sql

    def _add_serial_column(self, connection, *args, **kwargs):
        column_name = kwargs['params']['serial_name']
        rows = kwargs['rows']
        utils = kwargs['utils']

        for row in rows:
            schema_name = row['table_schema']
            table_name = row['table_name']
            data_type = row['data_type']

            table_name = self._build_table_name(schema_name, table_name)

            sql = self._build_sql_to_add_column(table_name, column_name, data_type)
            if sql is not None:
                utils.execute(connection, sql)

    def _add_temporary_column(self, connection, *args, **kwargs):
        rows = kwargs['rows']
        utils = kwargs['utils']
        for row in rows:
            schema_name = row['table_schema']
            table_name = row['table_name']
            column_name = row['column_name']

            table_name = self._build_table_name(schema_name, table_name)
            column_name = self._build_temp_column_name(column_name)

            sql = self._build_sql_to_add_column(table_name, column_name, 'UUID')
            if sql is not None:
                utils.execute(connection, sql)