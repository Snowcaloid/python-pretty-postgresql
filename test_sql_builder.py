from unittest import TestCase, main
from sql_builder import AF, KW, SQLAlias, SQLAlterTable, SQLColumnConstraint, SQLColumnForeignKey, SQLCreateTable, SQLSelect, SF, Exp, SQLTableColumn
from debugpy import listen, wait_for_client

class SelectTest(TestCase):
    def test_normal(self):
        self.assertEqual('SELECT * FROM tablename',
                         SQLSelect().fields([SF('*')]).table('tablename').compile())
        self.assertEqual("SELECT 1+1 AS two, a.column FROM tablename a",
                         SQLSelect().fields([SF('1+1', 'two'), SF('a.column')]) \
                             .table('tablename', 'a').compile())

    def test_where(self):
        self.assertEqual('SELECT * FROM tablename WHERE (a = 1)',
                         SQLSelect().fields([SF('*')]).table('tablename').where(Exp('a') == Exp(1)).compile())
        self.assertEqual('SELECT * FROM tablename WHERE ((a = 1) AND (b = 2))',
                         SQLSelect().fields([SF('*')]).table('tablename').where((Exp('a') == Exp(1)) & (Exp('b') == Exp(2))).compile())

    def test_order_by(self):
        self.assertEqual('SELECT * FROM tablename ORDER BY a',
                         SQLSelect().fields([SF('*')]).table('tablename').order_by([SF('a')]).compile())
        self.assertEqual('SELECT * FROM tablename ORDER BY a, b DESC',
                         SQLSelect().fields([SF('*')]).table('tablename').order_by([SF('a'), (SQLAlias('b'), False)]).compile())

    def test_join(self):
        self.assertEqual('SELECT * FROM tablename a INNER JOIN table2 ON (a.first = table2.second)',
                         SQLSelect().fields([SF('*')]) \
                             .table('tablename', 'a').join('table2', AF('a', 'first') == AF('table2', 'second')).compile())
        self.assertEqual('SELECT * FROM tablename a INNER JOIN table2 ON (a.first = table2.second) OUTER JOIN table3 b ON (b.first = table2.second)',
                         SQLSelect().fields([SF('*')]) \
                             .table('tablename', 'a').join('table2', AF('a', 'first') == AF('table2', 'second')) \
                             .join(('table3', 'b'), AF('b', 'first') == AF('table2', 'second'), KW.OUTER_JOIN).compile())
        self.assertEqual('SELECT * FROM tablename a LEFT JOIN table2 ON (a.first = table2.second) FULL OUTER JOIN table3 b ON (b.first = table2.second) RIGHT JOIN table4 c ON (c.first = table2.second)',
                         SQLSelect().fields([SF('*')]) \
                             .table('tablename', 'a').join('table2', AF('a', 'first') == AF('table2', 'second'), KW.LEFT_JOIN) \
                             .join(('table3', 'b'), AF('b', 'first') == AF('table2', 'second'), KW.FULL_OUTER_JOIN) \
                             .join(('table4', 'c'), AF('c', 'first') == AF('table2', 'second'), KW.RIGHT_JOIN).compile())

    def test_group_by(self):
        self.assertEqual('SELECT * FROM tablename GROUP BY a',
                         SQLSelect().fields([SF('*')]).table('tablename').group_by([SF('a')]).compile())
        self.assertEqual('SELECT * FROM tablename GROUP BY a, b',
                         SQLSelect().fields([SF('*')]).table('tablename').group_by([SF('a'), SF('b')]).compile())
        self.assertEqual('SELECT * FROM tablename GROUP BY a, b HAVING (a = 1)',
                         SQLSelect().fields([SF('*')]).table('tablename').group_by([SF('a'), SF('b')], Exp('a') == Exp(1)).compile())

    def test_limit(self):
        self.assertEqual('SELECT * FROM tablename LIMIT 1',
                         SQLSelect().fields([SF('*')]).table('tablename').limit(1).compile())
        self.assertEqual('SELECT * FROM tablename LIMIT 1 OFFSET 2',
                         SQLSelect().fields([SF('*')]).table('tablename').limit(1, 2).compile())

    def test_top(self):
        self.assertEqual('SELECT TOP 1 * FROM tablename',
                         SQLSelect().fields([SF('*')]).table('tablename').top(1).compile())
        self.assertEqual('SELECT TOP 1 PERCENT * FROM tablename',
                         SQLSelect().fields([SF('*')]).table('tablename').top(1, True).compile())

    def test_distinct(self):
        self.assertEqual('SELECT DISTINCT * FROM tablename',
                         SQLSelect(True).fields([SF('*')]).table('tablename').compile())

    def test_subquery(self):
        self.assertEqual('SELECT * FROM (SELECT * FROM tablename) a',
                         SQLSelect().fields([SF('*')]).table(SQLSelect(alias='a').fields([SF('*')]).table('tablename')).compile())
        self.assertEqual('SELECT * FROM tablename a WHERE (NOT (field2 IN (SELECT * FROM othertable b WHERE (a.field < b.field))))',
                            SQLSelect().fields([SF('*')]).table('tablename', 'a').where(
                                ~Exp('field2')._in((SQLSelect().fields([SF('*')]).table('othertable', 'b').where(Exp('a.field') < Exp('b.field'))))).compile())

class CreateTableTest(TestCase):
    def test_create_table(self):
        self.assertEqual('CREATE TABLE tablename (field1 INT, field2 VARCHAR(255))',
                         SQLCreateTable('tablename', [SQLTableColumn('field1', 'INT'), SQLTableColumn('field2', 'VARCHAR(255)')]).compile())
        self.assertEqual('CREATE TABLE IF NOT EXISTS tablename (field1 INT, CONSTRAINT pk PRIMARY KEY (field1), field2 VARCHAR(255))',
                        SQLCreateTable('tablename', [SQLTableColumn('field1', 'INT', [SQLColumnConstraint('pk', KW.PRIMARY_KEY, '(field1)')]),
                                                    SQLTableColumn('field2', 'VARCHAR(255)')], True).compile())
        self.assertEqual('CREATE TABLE tablename (field1 INT, CONSTRAINT pk PRIMARY KEY (field1), field2 VARCHAR(255), CONSTRAINT fk FOREIGN KEY (field2) REFERENCES othertable (field3))',
                        SQLCreateTable('tablename', [SQLTableColumn('field1', 'INT', [SQLColumnConstraint('pk', KW.PRIMARY_KEY, '(field1)')]),
                                                    SQLTableColumn('field2', 'VARCHAR(255)', [SQLColumnForeignKey('fk', 'field2', 'othertable', 'field3')])]).compile())

class AlterTableTest(TestCase):
    def test_add_column(self):
        self.assertEqual('ALTER TABLE tablename ADD COLUMN field1 INT',
                         SQLAlterTable('tablename').add_column('field1', 'INT'))
        self.assertEqual('ALTER TABLE tablename ADD COLUMN IF NOT EXISTS field1 INT',
                         SQLAlterTable('tablename').add_column('field1', 'INT', True))

    def test_drop_column(self):
        self.assertEqual('ALTER TABLE tablename DROP COLUMN field1',
                         SQLAlterTable('tablename').drop_column('field1'))
        self.assertEqual('ALTER TABLE tablename DROP COLUMN IF EXISTS field1',
                         SQLAlterTable('tablename').drop_column('field1', True))

    def test_alter_column(self):
        self.assertEqual('ALTER TABLE tablename ALTER COLUMN field1 BIGINT',
                         SQLAlterTable('tablename').alter_column('field1', 'BIGINT'))

    def test_rename_column(self):
        self.assertEqual('ALTER TABLE tablename RENAME COLUMN field1 TO field2',
                         SQLAlterTable('tablename').rename_column('field1', 'field2'))

    def test_add_constraint(self):
        self.assertEqual('ALTER TABLE tablename ADD CONSTRAINT pk PRIMARY KEY (field1)',
                         SQLAlterTable('tablename').add_constraint(SQLColumnConstraint('pk', KW.PRIMARY_KEY, '(field1)')))
        self.assertEqual('ALTER TABLE tablename ADD CONSTRAINT fk FOREIGN KEY (field1) REFERENCES othertable (field2)',
                         SQLAlterTable('tablename').add_constraint(SQLColumnForeignKey('fk', 'field1', 'othertable', 'field2')))

    def test_drop_constraint(self):
        self.assertEqual('ALTER TABLE tablename DROP CONSTRAINT pk',
                         SQLAlterTable('tablename').drop_constraint('pk'))
        self.assertEqual('ALTER TABLE tablename DROP CONSTRAINT IF EXISTS pk',
                         SQLAlterTable('tablename').drop_constraint('pk', True))

if __name__ == '__main__':
    listen(('localhost', 5678))
    wait_for_client()
    main()