import argparse
from os.path import join
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, stddev, expr, col, date_format
from os import remove

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_dates_from_weeks


def extract_e_recharge_data(spark, td_driver, td_url, td_user, td_password, beg_date, end_date, output_path):
    td_process = ExtractorSQL(spark, False, td_driver, td_url, td_user, td_password)
    e_recharge_sql = "SELECT ZB.TRANSFER_DATE_TIME AS D_TIME, "+\
"CASE WHEN LENGTH(to_char(ZB.RECEIVER_MSISDN))=9 THEN '2250'||to_char(ZB.RECEIVER_MSISDN) "+\
"   WHEN LENGTH(to_char(ZB.RECEIVER_MSISDN))=10 THEN '225'||to_char(ZB.RECEIVER_MSISDN) "+\
"   ELSE to_char(ZB.RECEIVER_MSISDN) "+\
"   END (VARCHAR(50)) MSISDN, "+\
"LV.LV_LABEL AS TYPE_TRANSACTION, "+\
"CAST(ZB.TRANSFER_AMOUNT AS DECIMAL(30,0)) AS TRANSFER_AMOUNT, "+\
"CAST(ZB.COMMISION AS DECIMAL(30,0)) AS COMMISSION, "+\
"ZB.DWH_TRANSFER_STATUS_LV AS TRANSFER_STATUS "+\
"FROM PGANV_DWH.CHANNEL_TRANSACTION ZB "+\
"LEFT JOIN PGANV_DWH.DWH_TABLE_LV LV ON (LV.LV_CODE=ZB.DWH_ZB_CHANNEL_TYPE_LV AND LV.LV_NAME='DWH_ZB_CHANNEL_TYPE_LV') WHERE CAST(ZB.TRANSFER_DATE_TIME AS DATE) BETWEEN CAST('" + beg_date + "'  AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "'  AS date FORMAT 'yyyyMMdd') AND ZB.DWH_TRANSFER_STATUS_LV='200'"

    td_process.load(e_recharge_sql)
    td_process.to_table("td_e_recharge")

    # Filtering the successful
    td_tmp = spark.sql("""select * from td_e_recharge where transfer_status not in ('5', '6')""")
    td_tmp.createOrReplaceTempView("td_e_recharge_success")

    # Normalizing the data
    td_df_oba_norm = spark.sql("""select concat("22507", right(msisdn,8)) as msisdn, extract(week from d_time) as week_nb, type_transaction, transfer_amount, commission from td_e_recharge_success""")
    td_df_oba_norm.createOrReplaceTempView("td_e_recharge_norm")

    # Filtering out oba clients data
    td_df_filtered = spark.sql("""select week_nb, msisdn, type_transaction, transfer_amount, commission from td_e_recharge_norm inner join oba_clients on td_e_recharge_norm.msisdn = oba_clients.Phone""")
    # td_df_filtered.show()

    # Grouping the data for stats
    td_df = td_df_filtered.groupby(["week_nb", "msisdn", "type_transaction"]).agg(count("transfer_amount").alias("nbr_transfer"), \
                                    avg("transfer_amount").alias("moyenne transfer_amount"), \
                                    stddev("transfer_amount").alias("std_dev transfer_amount"), \
                                    expr("percentile(transfer_amount, 0.00)").alias("0% transfer_amount"), \
                                    expr("percentile(transfer_amount, 0.25)").alias("25% transfer_amount"), \
                                    expr("percentile(transfer_amount, 0.5)").alias("50% transfer_amount"), \
                                    expr("percentile(transfer_amount, 0.75)").alias("75% transfer_amount"), \
                                    expr("percentile(transfer_amount, 1.0)").alias("100% transfer_amount"), \
                                    avg("commission").alias("moyenne commission"), \
                                    stddev("commission").alias("std_dev commission"), \
                                    expr("percentile(commission, 0.00)").alias("0% commission"), \
                                    expr("percentile(commission, 0.25)").alias("25% commission"), \
                                    expr("percentile(commission, 0.5)").alias("50% commission"), \
                                    expr("percentile(commission, 0.75)").alias("75% commission"), \
                                    expr("percentile(commission, 1.0)").alias("100% commission"))

    td_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_to_" + end_date + "_results.csv"
    extract_e_recharge_data(spark, td_driver, td_url, td_user, td_password, start_date, end_date, filename)

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
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_20210930.zip"
    destination_folder = "/home/davidtia/incoming/telco_data/activity_detail/e_recharge/"
    beg_date, end_date = generate_previous_week()
    # beg_date = "20211011"
    # end_date = "20211017"
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
    app_name = "PySpark OBA data extraction e recharge"
    driver_home = "/home/patrice.nzi/backup/oba/jars"
    td_driver = 'com.teradata.jdbc.TeraDriver'
    td_url = "jdbc:teradata://10.241.10.4/Database=GANDHI,LOGMECH=TD2"
    td_user = "pgan"
    td_password = "ocit"

    spark = SparkSession.builder.config("spark.executor.memory", "10g").\
        config("spark.driver.memory", "2g").\
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

    # check if it for new clients
    if str(args.clients).find("new") != -1: # contains new clients
        dates = generate_dates_from_weeks(26, args.edate)
        prefix = "oba_e_recharge_aggregation_newclients_of_" + args.edate + "_"
        for pair_of_dates in dates:
            sdate = pair_of_dates[0]
            edate = pair_of_dates[1]
            extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)
    else:
        sdate = args.sdate
        edate = args.edate

        if args.auto == "true":
            sdate = beg_date
            edate = end_date

        prefix = "oba_e_recharge_aggregation_"
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)

    spark.stop()


if __name__=="__main__":
    main()
