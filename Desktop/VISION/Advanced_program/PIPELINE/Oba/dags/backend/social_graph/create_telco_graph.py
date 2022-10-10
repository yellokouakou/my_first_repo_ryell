from graphframes import GraphFrame
from pyspark.sql.functions import count, sum as _sum, avg, stddev, expr
from pyspark.sql.types import StringType, StructField, StructType
from os.path import join
from pathlib import Path
from enum import Enum

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL


class Category(Enum):
    ROAMING = 1
    INTER = 2
    LOCAL = 3


class SVC(Enum):
    VOIX = 1
    DATA = 2
    SMS = 3
    SVA = 4
    IMS = 5
    UNKNOWN = 6


def foreign_number(msisdn):
    if isinstance(msisdn, str) and "225" not in msisdn[0:5]:
        return "EXT"
    else:
        return msisdn


class TelcoGraph(object):
    """GrapheFrame for Telco Transactions"""
    def __init__(self, spark, is_oracle, db_driver, db_url, db_user, db_password, beg_date, end_date, table, msisdn_table):
        self._spark = spark
        self._table = table
        self._msisdn_table = msisdn_table
        self._graph = None
        self._spark.udf.register("_setDestination", foreign_number, StringType())
        db_process = ExtractorSQL(spark, is_oracle, db_driver, db_url, db_user, db_password)
        sql = "SELECT RT.CALLING_NBR as CALLING_NBR, \
            RT.CALLED_NBR as CALLED_NBR, \
            RT.RE_ID as RE_ID, \
            RT.BILLING_NBR as MSISDN, \
            RT.DURATION as DURATION, BYTES_0, \
            TRIM(CASE RT.SERVICE_TYPE_LV WHEN 1 THEN 'DATA' WHEN 2 THEN 'VOIX' WHEN 4 THEN 'SMS' WHEN 10 THEN 'SVA' WHEN 512 THEN 'IMS' ELSE 'UNKNOWN' END(CHAR(50))) as SERVICE, \
            CASE RT.DIM_UNIVERSE_LV WHEN 1 THEN 'FIX' WHEN 2 THEN 'MOBILE' WHEN 3 THEN 'INTERNET' WHEN 4 THEN 'MOBILE/BANKING' ELSE 'UNKNOWN' END(char(15)) AS UNIVERSE, \
            (DECODE(RT.acct_res_id1,1,RT.Charge1,0)+DECODE(RT.acct_res_id2,1,RT.Charge2,0)+DECODE(RT.acct_res_id3,1,RT.Charge3,0)+DECODE(RT.acct_res_id4,1,RT.Charge4,0))/100 as CONSO_MB, \
            RA.RE_CATEGORY_LV as CATEGORIE \
            FROM PGANV_DWH.RATED_CDR_ALL RT \
            INNER JOIN PGANV_DWH.EXTENSION EXT ON (EXT.DWH_EXTENSION_ID=RT.CALLED_PREFIX_ID) \
            INNER JOIN PGANV_DWH.RATABLE_EVENT RA ON (RA.RE_ID=RT.RE_ID) \
            WHERE CAST(START_TIME AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "' AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "' AS date FORMAT 'yyyyMMdd')"
        db_process.load(sql)
        db_process.to_table(self._table)
        # spark.sql("select * from " + self._table).show()

    def _build_aggregated_data(self, re_id: str, universe: str, categorie: Category, svc: SVC):
        sql = "select calling_nbr as src, called_nbr as dst, msisdn as msisdn, service, duration, bytes_0 as bytes, conso_mb from " + self._table
        # self._spark.sql(sql).show()
         
        add_parameter = False
        if categorie == Category.INTER:
            if add_parameter:
                sql = sql + " and categorie like '%INTER%'"
            else:
                sql = sql + " where categorie like '%INTER%'"
                add_parameter = True
        elif categorie == Category.ROAMING:
            if add_parameter:
                sql = sql + " and categorie like '%ROAM%'"
            else:
                sql = sql + " where categorie like '%ROAM%'"
                add_parameter = True
        else:
            pass

        if re_id is not None and len(re_id) > 0:
            sql = sql + " where re_id='" + re_id + "'"
            add_parameter = True

        if universe is not None and len(universe) > 0:
            if add_parameter:
                sql = sql + " and universe='" + universe + "'"
            else:
                sql = sql + " where universe='" + universe + "'"

        # print(sql)
        data_filtered = self._spark.sql(sql)
        # data_filtered.show()
        data_filtered.createOrReplaceTempView("tmp_graph")

        if svc == SVC.VOIX:
            data_voix = self._spark.sql("select src, dst, duration from tmp_graph where service = 'VOIX'")
            graph_data_voix = data_voix.groupby(["src", "dst"]).agg(_sum("duration").alias("total_duration"))
            graph_data_voix.createOrReplaceTempView(self._table + "_voix")
        elif svc == SVC.DATA:
            data = self._spark.sql("select concat('22507', right(msisdn, 8)) as msisdn, bytes, conso_mb from tmp_graph where service = '" + str(svc.name) + "'") # check the conversion of enum to str.
            data.createOrReplaceTempView("data_norm")
            # Filtering out oba clients data
            data_filtered = self._spark.sql("""select msisdn, bytes, conso_mb from data_norm inner join oba_clients on data_norm.msisdn = oba_clients.Phone""")
            graph_data = data_filtered.groupby(["msisdn"]).agg(count("bytes").alias("nbr_exch_data"), \
                                        _sum("bytes").alias("total_bytes"), \
                                        avg("bytes").alias("moyenne_bytes"), \
                                        stddev("bytes").alias("std_dev bytes"), \
                                        expr("percentile(bytes, 0.00)").alias("0% bytes"), \
                                        expr("percentile(bytes, 0.25)").alias("25% bytes"), \
                                        expr("percentile(bytes, 0.5)").alias("50% bytes"), \
                                        expr("percentile(bytes, 0.75)").alias("75% bytes"), \
                                        expr("percentile(bytes, 1.0)").alias("100% bytes"), \
                                        _sum("conso_mb").alias("total_conso_mb"), \
                                        avg("conso_mb").alias("moyenne_conso_mb"), \
                                        stddev("conso_mb").alias("std_dev conso_mb"), \
                                        expr("percentile(conso_mb, 0.00)").alias("0% conso_mb"), \
                                        expr("percentile(conso_mb, 0.25)").alias("25% conso_mb"), \
                                        expr("percentile(conso_mb, 0.5)").alias("50% conso_mb"), \
                                        expr("percentile(conso_mb, 0.75)").alias("75% conso_mb"), \
                                        expr("percentile(conso_mb, 1.0)").alias("100% conso_mb"))
            graph_data.createOrReplaceTempView(self._table + "_data")
            self._spark.catalog.dropTempView("data_norm")
        elif svc == SVC.SMS:
            data = self._spark.sql("select src, dst, service from tmp_graph where service = '" + str(svc.name) + "'") # check the conversion of enum to str.
            suffix = "_" + str(svc.name).lower()
            als = "nbr_exch" + suffix
            graph_data = data.groupby(["src", "dst"]).agg(count("service").alias(als))
            graph_data.createOrReplaceTempView(self._table + suffix)
        else:
            # IMS, SVA, UNKNOWN
            data = self._spark.sql("select concat('22507', right(msisdn, 8)) as msisdn, service from tmp_graph where service = '" + str(svc.name) + "'") # check the conversion of enum to str.
            data.createOrReplaceTempView("data_norm")
            data_filtered = self._spark.sql("""select msisdn, service from data_norm inner join oba_clients on data_norm.msisdn = oba_clients.Phone""")
            suffix = "_" + str(svc.name).lower()
            als = "nbr_exch" + suffix
            graph_data = data_filtered.groupby("msisdn").agg(count("service").alias(als))
            graph_data.createOrReplaceTempView(self._table + suffix)
            self._spark.catalog.dropTempView("data_norm")

        self._spark.catalog.dropTempView("tmp_graph")

    def _build_client_list(self):
        msisdn_sql = "(select calling_nbr as id from " + self._table + ") union all (select called_nbr as id from " + self._table + ")"
        data_msisdn = self._spark.sql(msisdn_sql)
        data_msisdn.createOrReplaceTempView(self._msisdn_table)

    def build_telco_graphs(self, re_id: str, svc: SVC, categorie: Category):
        self._build_client_list()
        self._build_aggregated_data(re_id, "MOBILE", categorie, svc)
        if svc == SVC.SMS or svc == SVC.VOIX:
            vertices = self._spark.sql("select * from " + self._msisdn_table)
            tab = str(svc.name).lower()
            g_sql = "select * from " + self._table + "_" + tab
            edges = self._spark.sql(g_sql)
            g = GraphFrame(vertices, edges)
            return g
        return None

