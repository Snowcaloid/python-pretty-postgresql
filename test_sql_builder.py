from unittest import TestCase, main
from sql_builder import AF, KW, SQLAlias, SQLSelect, SF, Exp
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
if __name__ == '__main__':
    listen(('localhost', 5678))
    wait_for_client()
    main()