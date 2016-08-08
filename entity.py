import psycopg2
import psycopg2.extras

class DatabaseError(Exception):
    pass
class NotFoundError(Exception):
    pass


class Entity(object):
    db = None

    # ORM part 1
    __delete_query    = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query    = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
    __list_query      = 'SELECT * FROM "{table}"'
    __select_query    = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
    __update_query    = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'

    # # ORM part 2
    # __parent_query    = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
    # __sibling_query   = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'
    # __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor   = self.__class__.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )
        self.__fields   = {}
        self.__id       = id
        self.__loaded   = False
        self.__modified = False
        self.__table    = self.__class__.__name__.lower()

    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized
        if self.__modified:
            raise NotFoundError()
        self.__load()
        return self._get_column(name)

    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation
        if name in self._columns:
            self._set_column(name, value)
        else:
            super(Entity, self).__setattr__(name, value)

    def __execute_query(self, query, args):
        # execute an sql statement and handle exceptions together with transactions
        try:
            self.__cursor.execute(query, args)
        except Exception as e:
            self.__class__.db.rollback()
            raise DatabaseError
        else:
            self.__class__.db.commit()

    def __insert(self):
        # generate an insert query string from fields keys and values and execute it
        # use prepared statements
        # save an insert id
        insert_query = self.__insert_query.format(
            table=self.__table,
            columns=', '.join(self.__fields.keys()),
            placeholders=', '.join(['%s'] * len(self.__fields.keys()))
        )
        self.__execute_query(insert_query, self.__fields.values())
        self.__id = self.__cursor.fetchone()['{table}_id'.format(table=self.__table)]

    def __load(self):
        # if current instance is not loaded yet - execute select statement and store it's result as an associative array (fields), where column names used as keys
        if not self.__loaded:
            load = self.__select_query.format(table=self.__table)
            self.__execute_query(load, (self.__id,))
            self.__fields = self.__cursor.fetchone()
            self.__loaded = True

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements
        columns_list = ["{key} = '{value}'".format(key=key, value=value)
                        for key, value in self.__fields.items()]
        columns_str = ', '.join(columns_list)
        update = self.__update_query.format(table=self.__table, columns=columns_str)
        self.__execute_query(update, (self.__id,))
        # self.__modified = False

    def _get_children(self, name):
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        pass

    def _get_column(self, name):
        # return value from fields array by <table>_<name> as a key
        field_value = '{table}_{name}'.format(table=self.__table, name=name)
        if field_value not in self.__fields.keys():
            raise NotFoundError()
        return self.__fields[field_value]

    # def _get_parent(self, name):
    #     # ORM part 2
    #     # get parent id from fields with <name>_id as a key
    #     # return an instance of parent entity class with an appropriate id
    #     pass

    # def _get_siblings(self, name):
    #     # ORM part 2
    #     # get parent id from fields with <name>_id as a key
    #     # return an array of sibling entity instances
    #     # each sibling instance must have an id and be filled with data
    #     pass

    def _set_column(self, name, value):
        # put new value into fields array with <table>_<name> as a key
        self.__fields['{table}_{name}'.format(table=self.__table, name=name)] = value
        self.__modified = True

    # def _set_parent(self, name, value):
    #     # ORM part 2
    #     # put new value into fields array with <name>_id as a key
    #     # value can be a number or an instance of Entity subclass
    #     pass

    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corrensponding table
        # for each row create an instance of appropriate class
        # each instance must be filled with column data, a correct id and MUST NOT query a database for own fields any more
        # return an array of istances
        get_all = cls.__list_query.format(table=cls.__name__.lower())
        cursor = cls.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(get_all)
        instances_array = []
        for row in cursor:
            table_id = row['{table}_id'.format(table=cls.__name__.lower())]
            entity = cls(table_id)
            entity.__fields = row
            entity.__loaded = True
            instances_array.append(entity)
        return instances_array

    def delete(self):
        # execute delete query with appropriate id
        if not self.__id:
            raise NotFoundError()
        else:
            delete = self.__delete_query.format(table=self.__table)
            self.__execute_query(delete, (self.__id,))

    @property
    def id(self):
        # try to guess yourself
        return self.__id

    @property
    def created(self):
        # try to guess yourself
        return self.__fields[self.__table +'_created']

    @property
    def updated(self):
        # try to guess yourself
        return self.__fields[self.__table +'_updated']

    def save(self):
        # execute either insert or update query, depending on instance id
        if self.__id is None:
            self.__insert()
        else:
            self.__update()
        self.__modified = False
