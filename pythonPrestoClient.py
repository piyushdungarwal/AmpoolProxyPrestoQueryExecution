import presto
import warnings
from presto import transaction

warnings.filterwarnings('ignore')

with presto.dbapi.connect(
    host='172.31.5.86',
    port=9295,
    user='demokey',
    catalog='ampoolproxy',
    schema='default',
    http_scheme='https',
    auth=presto.auth.BasicAuthentication("demokey", "4YUkMPEoJZC15bUd"),
    #isolation_level=transaction.IsolationLevel.REPEATABLE_READ,
) as conn:
  conn._http_session.verify = False
  cur = conn.cursor()  

  cur.execute("SELECT s_name, count(*) AS numwait FROM ampoolproxy.tpch002.supplier_csv, ampoolproxy.tpch002.lineitem_gp L1, ampoolproxy.tpch002.orders_gp, ampoolproxy.tpch002.nation_parquet WHERE s_suppkey = L1.l_suppkey AND o_orderkey = L1.l_orderkey   AND o_orderstatus = 'F'   AND L1.l_receiptdate > L1.l_commitdate  AND exists( SELECT * FROM  ampoolproxy.tpch002.lineitem_gp L2 WHERE L2.l_orderkey = L1.l_orderkey AND L2.l_suppkey <> L1.l_suppkey ) AND NOT exists( SELECT * FROM  ampoolproxy.tpch002.lineitem_gp L3   WHERE L3.l_orderkey = L1.l_orderkey AND L3.l_suppkey <> L1.l_suppkey  AND L3.l_receiptdate > L3.l_commitdate )   AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100")
  rows = cur.fetchall()
  print rows

  '''
  cur.execute('CREATE TABLE tab1 (a int, b int, c varchar)')
  rows = cur.fetchall()
  print rows

  cur.execute('INSERT INTO tab1 VALUES (1, 3, \'abc\')')
  rows = cur.fetchall()
  print rows

  cur.execute('INSERT INTO tab1 VALUES (4, 6, \'def\')')
  rows = cur.fetchall()
  print rows
  
  cur.execute('SELECT * FROM tab1')
  rows = cur.fetchall()
  print rows
  
  cur.execute('SELECT cast(a as varchar) as a, cast(b as double) as b, upper(c) as c FROM tab1')
  rows = cur.fetchall()
  print rows

  cur.execute('DROP TABLE tab1')
  rows = cur.fetchall()
  print rows
  '''
