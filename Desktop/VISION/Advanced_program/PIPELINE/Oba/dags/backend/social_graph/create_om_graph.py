from graphframes import GraphFrame
from pyspark.sql.functions import count, sum as _sum
from os.path import join
from pathlib import Path


import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL


class OmGraph(object):
    """GrapheFrame for Orange Money Transactions"""
    def __init__(self, spark, is_oracle, db_driver, db_url, db_user, db_password, beg_date, end_date, table, msisdn_table):
        self._spark = spark
        self._table = table
        self._msisdn_table = msisdn_table
        self._graph = None
        db_process = ExtractorSQL(spark, is_oracle, db_driver, db_url, db_user, db_password)
        sql = "SELECT SENDER_MSISDN, RECEIVER_MSISDN, TRANSACTION_AMOUNT, SERVICE_CHARGE_AMOUNT, TRANSACTION_TAG FROM PGANV_ODS.TANGO_APGL WHERE CAST(TRANSACTION_DATE_TIME AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "' AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "' AS date FORMAT 'yyyyMMdd') AND TRANSACTION_STATUS='TS' AND ROLLBACKED IS NULL AND SENDER_CATEGORY_CODE='SUBS' AND RECEIVER_CATEGORY_CODE='SUBS'"
        db_process.load(sql)
        db_process.to_table(self._table)

    def _build_aggregated_data(self, tag: str):
        sql = "select sender_msisdn as src, receiver_msisdn as dst, transaction_amount, service_charge_amount, transaction_tag from " + self._table 

        if tag is not None and len(tag) > 0:
            sql = sql + " where transaction_tag='" + tag + "'"

        data_filtered = self._spark.sql(sql)
        data_graph = data_filtered.groupby(["src", "dst", "transaction_tag"]).agg(count("transaction_amount").alias("nbr_transactions"), \
                                    _sum("transaction_amount").alias("total_transaction"), \
                                    count("service_charge_amount").alias("nbr_service_charge"), \
                                    _sum("service_charge_amount").alias("total_service_charge"))
        data_graph.createOrReplaceTempView(self._table)

    def _build_client_list(self):
        msisdn_sql = "(select sender_msisdn as id from " + self._table + ") union all (select receiver_msisdn as id from " + self._table + ")"
        data_msisdn = self._spark.sql(msisdn_sql)
        data_msisdn.createOrReplaceTempView(self._msisdn_table)

    def build_om_graph(self, tag: str):
        self._build_client_list()
        self._build_aggregated_data(tag)
        vertex = self._spark.sql("select * from " + self._msisdn_table)
        edges = self._spark.sql("select * from " + self._table)
        return GraphFrame(vertex, edges)

