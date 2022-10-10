import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, lit, stddev, expr, sum as _sum
from pyspark.sql.types import StringType, StructField, StructType
import pandas as pd
from os import remove
from os.path import join
from pathlib import Path
from enum import Enum

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))


from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_previous_dates, generate_dates_from_weeks
from extractor import ExtractorSQL


def format_number(msisdn):
    is_civ = str_nbr.find("225")
    transformed = str_nbr
    if ((is_civ != -1 and is_civ < 5) or len(transformed) <= 10):
        transformed = "22507" + transformed[-8:] # because we are only interested in oci numbers so there is no issue as we will join with oba clients.
    return transformed


class Category(Enum):
    ROAMING = 1
    INTER = 2
    LOCAL = 3


def extract_sms_telco_data(spark, td_driver, td_url, td_user, td_password, re_id, categorie, beg_date, end_date, output_path):
    db_process = ExtractorSQL(spark, False, td_driver, td_url, td_user, td_password)
    sql = "SELECT RT.CALLING_NBR as CALLING_NBR, \
            RT.CALLED_NBR as CALLED_NBR, \
            RT.RE_ID as RE_ID \
            FROM PGANV_DWH.RATED_CDR_ALL RT \
            INNER JOIN PGANV_DWH.EXTENSION EXT ON (EXT.DWH_EXTENSION_ID=RT.CALLED_PREFIX_ID) \
            INNER JOIN PGANV_DWH.RATABLE_EVENT RA ON (RA.RE_ID=RT.RE_ID) \
            WHERE CAST(START_TIME AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "' AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "' AS date FORMAT 'yyyyMMdd') AND RT.DIM_UNIVERSE_LV=2 \
            AND RT.SERVICE_TYPE_LV=3"

    if categorie == Category.INTER:
        sql = sql + " AND RA.RE_CATEGORY_LV LIKE '%INTER%'"
    elif categorie == Category.ROAMING:
        sql = sql + " AND RA.RE_CATEGORY_LV LIKE '%ROAM%'"
    else:
        pass

    if re_id is not None and len(re_id) > 0:
        sql = sql + " AND RT.RE_ID='" + re_id + "'"

    db_process.load(sql)
    db_process.to_table("telco_sms")

    data = spark.sql("select _formatNumber(calling_nbr) as src_msisdn, _formatNumber(called_nbr) as dst_msisdn from telco_sms")
    tr_data = data.groupby(["src_msisdn", "dst_msisdn"]).agg(count(lit(1)).alias("nbr_exchange"))
    tr_data.createOrReplaceTempView("telco_sms")
    out_filtered_data = spark.sql("""select src_msisdn, dst_msisdn, nbr_exchange from telco_sms inner join oba_clients on telco_sms.src_msisdn = oba_clients.Phone""")
    out_sms = out_filtered_data.groupby(["src_msisdn"]).agg(count("dst_msisdn").alias("nbr_dst"), \
                                        _sum("nbr_exchange").alias("total_sms_out"), \
                                        avg("nbr_exchange").alias("moyenne_sms_out"), \
                                        stddev("nbr_exchange").alias("std_dev_sms_out"), \
                                        expr("percentile(nbr_exchange, 0.00)").alias("0_per_sms_out"), \
                                        expr("percentile(nbr_exchange, 0.25)").alias("25_per_sms_out"), \
                                        expr("percentile(nbr_exchange, 0.5)").alias("50_per_sms_out"), \
                                        expr("percentile(nbr_exchange, 0.75)").alias("75_per_sms_out"), \
                                        expr("percentile(nbr_exchange, 1.0)").alias("100_per_sms_out"))
    out_sms.createOrReplaceTempView("out_sms")
    out_sms = spark.sql("""select Phone as msisdn, nbr_dst, total_sms_out, moyenne_sms_out, std_dev_sms_out, 0_per_sms_out, 25_per_sms_out, 50_per_sms_out, 75_per_sms_out, 100_per_sms_out from out_sms full join oba_clients on out_sms.src_msisdn = oba_clients.Phone""")
    out_sms.createOrReplaceTempView("out_sms")

    in_filtered_data = spark.sql("""select src_msisdn, dst_msisdn, nbr_exchange from telco_sms inner join oba_clients on telco_sms.dst_msisdn = oba_clients.Phone""")
    in_sms = in_filtered_data.groupby(["dst_msisdn"]).agg(count("src_msisdn").alias("nbr_src"), \
                                        _sum("nbr_exchange").alias("total_sms_in"), \
                                        avg("nbr_exchange").alias("moyenne_sms_in"), \
                                        stddev("nbr_exchange").alias("std_dev_sms_in"), \
                                        expr("percentile(nbr_exchange, 0.00)").alias("0_per_sms_in"), \
                                        expr("percentile(nbr_exchange, 0.25)").alias("25_per_sms_in"), \
                                        expr("percentile(nbr_exchange, 0.5)").alias("50_per_sms_in"), \
                                        expr("percentile(nbr_exchange, 0.75)").alias("75_per_sms_in"), \
                                        expr("percentile(nbr_exchange, 1.0)").alias("100_per_sms_in"))
    in_sms.createOrReplaceTempView("in_sms")
    in_sms = spark.sql("""select Phone as msisdn, nbr_src, total_sms_in, moyenne_sms_in, std_dev_sms_in, 0_per_sms_in, 25_per_sms_in, 50_per_sms_in, 75_per_sms_in, 100_per_sms_in from in_sms full join oba_clients on in_sms.dst_msisdn = oba_clients.Phone""")
    in_sms.createOrReplaceTempView("in_sms")

    agg_data = in_sms.join(out_sms, ["msisdn"])
    spark.catalog.dropTempView("telco_sms")
    agg_data.dropna(how='all')
    # agg_data.show()
    agg_data.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    for categorie in [Category.LOCAL, Category.INTER, Category.ROAMING]:
        suf = "_" + str(categorie.name).lower()
        filename = prefix +  start_date + "_to_" + end_date + suf + "_results.csv"
        extract_sms_telco_data(spark, td_driver, td_url, td_user, td_password, None, categorie, start_date, end_date, filename)

        # copying the data to server
        remote_path = args.dest + filename
        fast_ssh = FastSshCopy(args.host, args.port, args.user, args.password)
        fast_ssh.connect()
        fast_ssh.send(filename, remote_path)

        # removing the file
        print("file : " + filename + " has been copied to OBA!")
        remove(filename)
        print("file : " + filename + " has been removed!")

def main():
    # Defining the value of some variables
    host = '172.16.101.205'
    keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABgQC7IomqG+usVr+Cy0HJ/h47q6jxJ8A+poAAv5Eo4jH2yeTxgusXAt4FosjPKchxHCADNus7uNBOYNcQtiznOwAmhWIdFEWAO8dubJooyhhz+0+51VSID7THhfjvkBJDroRimwWd16mPXq8nA45JfxPSx5z7qWVOqz7B4aMXaOeODFEU/QJ44+8o3cqjdXsVqTgOvH2whKmvPx837vkjidSi5QJEM53B28OUzpMn9KHa6Hx4rYQtjKEPKbmNfnCooPtzpM4cZQXHd3H3Ga4Wln3LXbVvP2eQAKdXwqhxfJ8u9XO5kHOnGN8Iv25BuTvQ5YkMH/wtu26YWJUCb+M0bJl3Gjuq/zAmUrGxEmejl+fqkbiHCPcRK9FA9x4PKoONPqcT3RJ4SSKbyDGrjYKDICxgx3NTkg7l1Hs4RRphw5yGuXQYEOcb2PW+VnecoBB5RbE7S3NLoQdRV+O9v6ISmpDkqvpSfZahfdD8T2AJKwy1Nvu6oCZOIJNdvILO4KwksJM="""
    port = 22
    user = "davidtia"
    password = "********"
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_old_last.csv"
    destination_folder = "/home/davidtia/incoming/telco_data/graph_telco/"
    # beg_date, end_date = generate_previous_week()
    beg_date = "20220207"
    end_date = "20220213"
    auto = "false"

    # Arguments parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--oba_clients', dest='clients', default=source_client_file, help="Source file to copy")
    parser.add_argument('--destination', dest='dest', default=destination_folder, help="Remote destination file to copy to")
    parser.add_argument('--host', dest='host', default=host, help='Host to connect to')
    parser.add_argument('--host-key', dest='hostkey', default=keydata, help='Host to connect to')
    parser.add_argument('--port', dest='port', default=port, help="Port to connect on", type=int)
    parser.add_argument('-u', dest='user', default=user, help="User name to authenticate as")
    parser.add_argument('-p', dest='password', default=password, help="Password for user to authenticate as")
    parser.add_argument('-start-date', dest='sdate', default=beg_date, help="start date for data")
    parser.add_argument('-end-date', dest='edate', default=end_date, help="end date for data")
    parser.add_argument('--auto', dest='auto', default=auto, help="generation automatique des dates ?")

    args = parser.parse_args()

    # ------------------------------------------
    # create an instance of SparkSession object
    # ------------------------------------------
    app_name = "PySpark OBA data extraction om"
    driver_home = "/home/patrice.nzi/backup/oba/jars"
    td_driver = 'com.teradata.jdbc.TeraDriver'
    td_url = "jdbc:teradata://10.241.10.4/Database=GANDHI,LOGMECH=TD2"
    td_user = "pgan"
    td_password = "ocit"

    spark = SparkSession.builder.config("spark.executor.memory", "8g").\
        config("spark.driver.memory", "6g").\
        config("spark.memory.offHeap.enabled", True).\
        config("spark.memory.offHeap.size", "2g").\
        config("spark.jars", driver_home + "/terajdbc4.jar").\
        appName(app_name).getOrCreate()

    # SSH connection
    ssh = SshCopy(args.host, args.hostkey, args.port, args.user, args.password)
    ssh.connect()

    # getting the oba client list
    path_to_oba_clients = "AllCustomers_last.csv"
    ssh.get(path_to_oba_clients, args.clients)
    clients_data = pd.read_csv(path_to_oba_clients, sep=";")
    spark.createDataFrame(clients_data).createOrReplaceTempView("oba_clients")

    spark.udf.register("_formatNumber", format_number, StringType())

    dates = generate_dates_from_weeks(59, args.edate)
    prefix = "oba_sms_telco_indicators_"
    for pair_of_dates in dates:
        sdate = pair_of_dates[0]
        edate = pair_of_dates[1]
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)

    spark.stop()


if __name__=="__main__":
    main()

