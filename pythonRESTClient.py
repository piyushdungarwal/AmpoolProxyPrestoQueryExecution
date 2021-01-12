#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import os
import warnings


def execute_query(
    url,
    payload,
    headers,
    prepare=False,
    ):
    cwd = os.getcwd()

    print '=============================================='
    print 'Query: ' + payload
    print '=============================================='

    response = requests.request('POST', url, headers=headers,
                                data=payload, verify=False)
    datajson = response.json()
    nextURL = datajson.get('nextUri')

    prepareHeader = ''
    result = []
    while nextURL is not None:
        resp = requests.request('GET', nextURL, headers=headers,
                                verify=False)

        if prepare == True and 'X-Presto-Added-Prepare' \
            in resp.headers.keys():
            prepareHeader = resp.headers['X-Presto-Added-Prepare']

    # print resp

        resp = resp.json()
        nextURL = resp.get('nextUri')

        if resp.get('stats').get('state') != 'FAILED':
            datalist = resp.get('data')
            if datalist is not None:
                result.extend(datalist)

    if prepare == True:
        return prepareHeader

    return result


def test_presto():
    warnings.filterwarnings('ignore')

    # Update AE master node ip here.
    url = 'https://172.31.5.86:9295/v1/statement'

    # 'YWRtaW46YWRtaW4=' is base64 encoded value of Presto username:Presto password. admin:admin in this case. Please update.
    authHeaders = {'Authorization': 'Basic ZGVtb2tleTo0WVVrTVBFb0paQzE1YlVk'}

    query = "SELECT s_name, count(*) AS numwait FROM ampoolproxy.tpch002.supplier_csv, ampoolproxy.tpch002.lineitem_gp L1, ampoolproxy.tpch002.orders_gp, ampoolproxy.tpch002.nation_parquet WHERE s_suppkey = L1.l_suppkey AND o_orderkey = L1.l_orderkey   AND o_orderstatus = 'F'   AND L1.l_receiptdate > L1.l_commitdate  AND exists( SELECT * FROM  ampoolproxy.tpch002.lineitem_gp L2 WHERE L2.l_orderkey = L1.l_orderkey AND L2.l_suppkey <> L1.l_suppkey ) AND NOT exists( SELECT * FROM  ampoolproxy.tpch002.lineitem_gp L3   WHERE L3.l_orderkey = L1.l_orderkey AND L3.l_suppkey <> L1.l_suppkey  AND L3.l_receiptdate > L3.l_commitdate )   AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"

    headers = {'Content-Type': 'text/plain',
                'X-Presto-User': 'ampool'}
    headers.update(authHeaders)

    result = execute_query(url, query, headers)
    print result
    print '(' + str(len(result)) + ' rows in result)'


test_presto()
