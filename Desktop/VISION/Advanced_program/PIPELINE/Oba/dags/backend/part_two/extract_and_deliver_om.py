import argparse
from os.path import join, dirname
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, stddev, expr
from os import remove

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_dates_from_weeks


def extract_om_data(spark, td_driver, td_url, td_user, td_password, beg_date, end_date, output_path):
    td_process = ExtractorSQL(spark, False, td_driver, td_url, td_user, td_password)
    om_sql = "SELECT SENDER_MSISDN, RECEIVER_MSISDN, SENDER_USER_ID, RECEIVER_USER_ID, TRANSACTION_STATUS, SENDER_PRE_BALANCE, SENDER_POST_BALANCE, RECEIVER_PRE_BALANCE, RECEIVER_POST_BALANCE, TRANSACTION_DATE_TIME, SENDER_CATEGORY_CODE, RECEIVER_CATEGORY_CODE, TRANSACTION_AMOUNT, SERVICE_CHARGE_AMOUNT, TRANSACTION_TAG, ROLLBACKED FROM PGANV_ODS.TANGO_APGL WHERE CAST(TRANSACTION_DATE_TIME AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "'  AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "'  AS date FORMAT 'yyyyMMdd')"
    td_process.load(om_sql)
    td_process.to_table("td_om_apgl")

    # Filtering the successful
    td_tmp = spark.sql("""select * from td_om_apgl where transaction_status = 'TS' and rollbacked is null""")
    td_tmp.createOrReplaceTempView("td_om_apgl_success")

    # Exploding the data
    td_df_exploded = spark.sql("""(select sender_msisdn as msisdn, 1 as is_send, sender_user_id as om_user_id, sender_pre_balance as pre_balance, sender_post_balance as post_balance, sender_category_code as category_code, receiver_category_code as other_category_code, transaction_amount as amount, service_charge_amount as service_charge, transaction_tag as tag from td_om_apgl_success) union all (select receiver_msisdn as msisdn, 0 as is_send, receiver_user_id as om_user_id, receiver_pre_balance as pre_balance, receiver_post_balance as post_balance, receiver_category_code as category_code, sender_category_code as other_category_code, transaction_amount as amount, service_charge_amount as service_charge, transaction_tag as tag from td_om_apgl_success)""")
    td_df_exploded.createOrReplaceTempView("td_om_apgl_exploded")

    # Normalizing the data
    td_df_oba_norm = spark.sql("""select concat("22507", right(msisdn,8)) as msisdn, is_send, om_user_id, pre_balance, post_balance, category_code, other_category_code, amount, service_charge, tag from td_om_apgl_exploded""")
    td_df_oba_norm.createOrReplaceTempView("td_df_oba_norm")

    # Filtering out oba clients data
    td_df_filtered = spark.sql("""select msisdn, om_user_id as om_id, is_send, category_code, other_category_code, tag, pre_balance, post_balance, amount, service_charge from td_df_oba_norm inner join oba_clients on td_df_oba_norm.msisdn = oba_clients.Phone""")
    td_df_filtered.show()

    # Grouping the data for stats
    td_df = td_df_filtered.groupby(["msisdn", "om_id", "is_send", "tag", "category_code", "other_category_code"]).agg(count("pre_balance").alias("nbr transactions"), \
                                    avg("pre_balance").alias("moyenne pre_balance"), \
                                    stddev("pre_balance").alias("std_dev pre_balance"), \
                                    expr("percentile(pre_balance, 0.00)").alias("0% pre_balance"), \
                                    expr("percentile(pre_balance, 0.25)").alias("25% pre_balance"), \
                                    expr("percentile(pre_balance, 0.5)").alias("50% pre_balance"), \
                                    expr("percentile(pre_balance, 0.75)").alias("75% pre_balance"), \
                                    expr("percentile(pre_balance, 1.0)").alias("100% pre_balance"), \
                                    avg("post_balance").alias("moyenne post_balance"), \
                                    stddev("post_balance").alias("std_dev post_balance"), \
                                    expr("percentile(post_balance, 0.00)").alias("0% post_balance"), \
                                    expr("percentile(post_balance, 0.25)").alias("25% post_balance"), \
                                    expr("percentile(post_balance, 0.5)").alias("50% post_balance"), \
                                    expr("percentile(post_balance, 0.75)").alias("75% post_balance"), \
                                    expr("percentile(post_balance, 1.0)").alias("100% post_balance"), \
                                    avg("amount").alias("moyenne amount"), \
                                    stddev("amount").alias("std_dev amount"), \
                                    expr("percentile(amount, 0.00)").alias("0% amount"), \
                                    expr("percentile(amount, 0.25)").alias("25% amount"), \
                                    expr("percentile(amount, 0.5)").alias("50% amount"), \
                                    expr("percentile(amount, 0.75)").alias("75% amount"), \
                                    expr("percentile(amount, 1.0)").alias("100% amount"), \
                                    avg("service_charge").alias("moyenne service_charge"), \
                                    stddev("service_charge").alias("std_dev service_charge"), \
                                    expr("percentile(service_charge, 0.00)").alias("0% service_charge"), \
                                    expr("percentile(service_charge, 0.25)").alias("25% service_charge"), \
                                    expr("percentile(service_charge, 0.5)").alias("50% service_charge"), \
                                    expr("percentile(service_charge, 0.75)").alias("75% service_charge"), \
                                    expr("percentile(service_charge, 1.0)").alias("100% service_charge"))

    td_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_to_" + end_date + "_results.csv"
    extract_om_data(spark, td_driver, td_url, td_user, td_password, start_date, end_date, filename)

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
    destination_folder = "/home/davidtia/incoming/recurrence_tmp/"
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_20210930.zip"
    beg_date, end_date = generate_previous_week()
    auto = "false"
    # beg_date = "20210906"
    # end_date = "20210912"

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
    driver_home = "/home/patrice.nzi/backup/oba/airflow_prod/dags/jars/"
    td_driver = 'com.teradata.jdbc.TeraDriver'
    td_url = "jdbc:teradata://10.241.10.4/Database=GANDHI,LOGMECH=TD2"
    td_user = "pgan"
    td_password = "ocit"

    spark = SparkSession.builder.config("spark.executor.memory", "10g").\
        config("spark.driver.memory", "4g").\
        config("spark.memory.offHeap.enabled", True).\
        config("spark.memory.offHeap.size", "2g").\
        config("spark.jars", driver_home + "terajdbc4.jar").\
        appName(app_name).getOrCreate()

    # SSH connection
    ssh = SshCopy(args.host, args.hostkey, args.port, args.user, args.password)
    ssh.connect()

    # getting the oba client list
    path_to_oba_clients = "AllCustomers_last.csv"
    ssh.get(path_to_oba_clients, args.clients)
    clients_data = pd.read_csv(path_to_oba_clients, sep=";")
    clients = spark.createDataFrame(clients_data)
    clients.createOrReplaceTempView("oba_clients")

    # check if it for new clients
    if str(args.clients).find("new") != -1: # contains new clients
        dates = generate_dates_from_weeks(26, args.edate)
        prefix = "activity_data_detailed_om_transactions_newclients_of_" + args.edate + "_"
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

        prefix = "activity_data_detailed_om_transactions_"
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)

    spark.stop()


if __name__=="__main__":
    main()
