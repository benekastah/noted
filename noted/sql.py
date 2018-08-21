import datetime
import dateutil.parser
import re
import sqlite3


def get_conn():
    if get_conn._conn:
        try:
            # Ensure the connection is open
            get_conn._conn.execute('select 1')
        except sqlite3.ProgrammingError:
            get_conn._conn = None

    if not get_conn._conn:
        from noted.core import get_config
        db_file = get_config().get('noted', 'db', fallback='journal.db')
        get_conn._conn = sqlite3.connect(db_file)

    return get_conn._conn

get_conn._conn = None


class SQLBuilder:
    def __init__(self, sql=''):
        self.sql_buffer = [sql]
        self.param_buffer = []

    def add(self, obj):
        sql, params = obj.build()
        self.sql_buffer.append(sql)
        self.param_buffer.extend(params)
        return self

    def add_sql(self, sql):
        return self.add(RawSQL(sql))

    def add_identifier(self, ident):
        return self.add(Identifier(ident))

    def add_literal(self, lit):
        return self.add(Literal(lit))

    def build(self):
        sql = ''.join(self.sql_buffer)
        params = tuple(self.param_buffer)
        return sql, params

    def execute(self, c):
        """
        Given a sqlite3 Connection or Cursor object, it builds the query and
        then executes it, returning the Cursor instance.
        """
        sql, params = self.build()
        curs = c.execute(sql, params)
        return curs


class SQLPart:
    def __init__(self, sql):
        self.input = sql

    def build(self):
        raise NotImplementedError


class RawSQL(SQLPart):
    def __init__(self, sql):
        self.input = sql
        self.raw_sql = sql

    def build(self):
        return self.input, ()


class Identifier(SQLPart):
    def __init__(self, ident):
        super().__init__(ident)

    def build(self):
        return '"{}"'.format(re.sub(r'"', '\\"', self.input)), ()


class Literal(SQLPart):
    def __init__(self, literal):
        super().__init__(literal)

    def build(self):
        return '?', (self.input,)


class PList(SQLPart):
    def __init__(self, objects, prefix='(', postfix=')'):
        super().__init__(list(objects))
        self.prefix = prefix
        self.postfix = postfix

    def build(self):
        builder = SQLBuilder(self.prefix)
        for i, obj in enumerate(self.input):
            if i != 0:
                builder.add_sql(', ')
            builder.add(obj)
        builder.add_sql(self.postfix)
        return builder.build()


class List(PList):
    def __init__(self, objects):
        super().__init__(objects, prefix='', postfix='')


def type_to_sql_type(typ):
    if issubclass(typ, int):
        return 'INTEGER'
    if issubclass(typ, float):
        return 'DOUBLE'
    if issubclass(typ, str):
        return 'TEXT'
    if issubclass(typ, bytes):
        return 'BLOB'
    if issubclass(typ, datetime.date):
        return 'DATE'
    if issubclass(typ, datetime.datetime):
        return 'DATETIME'


def value_to_type(value, typ):
    if value is None:
        return value
    if issubclass(typ, (int, float)):
        return typ(value)
    if issubclass(typ, str):
        if isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode()
        else:
            return str(value)
    if issubclass(typ, bytes):
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode()
        else:
            return bytes(value)
    if issubclass(typ, datetime.datetime):
        return dateutil.parser.parse(value)
    if issubclass(typ, datetime.date):
        return dateutil.parser.parse(value).date()


class Model:
    fields = {}

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def create_table(cls):
        with get_conn() as conn:
            fields = []
            for field, typ in cls.fields.items():
                sql_type = type_to_sql_type(typ)
                if sql_type:
                    fields.append(
                        SQLBuilder().
                        add(Identifier(field)).
                        add_sql(' ').
                        add(Identifier(sql_type)))
                else:
                    fields.append(Identifier(field))
            builder = (
                SQLBuilder('CREATE VIRTUAL TABLE IF NOT EXISTS ').
                add_identifier(cls.name).
                add_sql(' USING fts4').
                add(PList(fields))
            )
            builder.execute(conn)

    @classmethod
    def query(cls):
        with get_conn() as conn:
            fields = ('rowid',) + tuple(cls.fields.keys())
            curs = (
                SQLBuilder('SELECT ').
                add(List(Identifier(field) for field in fields)).
                add_sql(' FROM ').
                add_identifier(cls.name)
            ).execute(conn)
            for row in curs:
                yield cls.from_row(fields, row)

    @classmethod
    def from_row(cls, fields, row):
        inst = cls()
        for field, value in zip(fields, row):
            if field in cls.fields:
                value = value_to_type(value, cls.fields[field])
            setattr(inst, field, value)
        return inst

    def __init__(self, **kwargs):
        keys = set(self.fields.keys())
        self.rowid = kwargs.get('rowid')
        for field in keys:
            setattr(self, field, None)
        for key, value in kwargs.items():
            if key in keys or key == 'rowid':
                setattr(self, key, value)
            else:
                raise ValueError('No such field "{}" on table "{}"'.format(key, self.name))

    def __repr__(self):
        return '{} {}'.format(self.__class__.__name__, repr(list(self)))

    def __getitem__(self, i):
        field = list(self.fields.keys())[i]
        return getattr(self, field)

    def __len__(self):
        return len(self.fields)

    def insert(self):
        with get_conn() as conn:
            curs = (
                SQLBuilder('INSERT INTO ').add_identifier(self.name).
                add(PList(Identifier(field) for field in self.fields.keys())).
                add_sql(' VALUES ').
                add(PList(Literal(val) for val in self))
            ).execute(conn)
            self.rowid = curs.lastrowid

    def update(self):
        with get_conn() as conn:
            curs = (
                SQLBuilder('UPDATE ').add_identifier(self.name).
                add(PList(Identifier(field) for field in self.fields.keys())).
                add_sql(' = ').
                add(PList(Literal(val) for val in self)).
                add_sql(' WHERE rowid = ').add_literal(self.rowid)
            ).execute(conn)

    def save(self):
        if self.rowid:
            self.update()
        else:
            self.insert()
