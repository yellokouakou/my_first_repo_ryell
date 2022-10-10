import argparse
from os.path import join
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import avg, count, stddev, expr, col, date_format
from os import remove

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_dates_from_weeks


SUBSCRIBERS = {"1":"PREPAID", "2":"POSTPAID", "3":"HYBRID", "4":"OTHERS"}


def transform_subs(subs_type):
    subs = str(subs_type)
    if subs in ("1", "2", "3", "4"):
        label = SUBSCRIBERS[subs]
        return label
    return "OTHERS"


def get_tag_value(channel, benefit, value, tag):
    channel = str(channel)
    benefit = str(benefit)
    tag = str(tag)
    if channel in ("5", "30") and benefit.upper() == tag.upper():
        return value
    elif tag.upper() != "":
        return 0
    else:
        return value


def register_utils_udfs(spark):
    spark.udf.register("_prettifySubsUDF", transform_subs, StringType())
    spark.udf.register("_getTagValueUDF", get_tag_value, FloatType())


def extract_recharge_data(spark, td_driver, td_url, td_user, td_password, beg_date, end_date, output_path):
    td_process = ExtractorSQL(spark, False, td_driver, td_url, td_user, td_password)
    recharge_sql = "SELECT ACC_NBR AS MSISDN, BILL_AMOUNT AS AMOUNT, CONTACT_CHANNEL_ID, BENEFIT_NAME, SUBSCRIBER_TYPE_LV, INITIAL_LOAN_AMOUNT, AMOUNT_ALREADY_PAY_BACK, AMOUNT_REMAIN_TO_PAY_BACK from PGANV_DWH.RECHARGES_ALL WHERE CAST(PAY_TIME AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "'  AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "'  AS date FORMAT 'yyyyMMdd')"
    td_process.load(recharge_sql)
    td_process.to_table("td_recharge")

    # Normalizing the msisdn
    td_df_oba_norm = spark.sql("""select concat("22507", right(msisdn,8)) as msisdn, amount, contact_channel_id, benefit_name, subscriber_type_lv, initial_loan_amount, amount_already_pay_back, amount_remain_to_pay_back from td_recharge""")
    td_df_oba_norm.createOrReplaceTempView("td_df_oba_norm")

    # Filtering out oba clients data
    td_df_filtered = spark.sql("""select msisdn, amount, contact_channel_id, benefit_name, subscriber_type_lv, initial_loan_amount, amount_already_pay_back, amount_remain_to_pay_back from td_df_oba_norm inner join oba_clients on td_df_oba_norm.msisdn = oba_clients.Phone""")
    td_df_filtered.createOrReplaceTempView("td_df_filtered")

    # Exploding and normalizing the data
    td_df_oba_exploded = spark.sql("""select msisdn, _prettifySubsUDF(subscriber_type_lv) as subscriber_type, _getTagValueUDF(contact_channel_id, benefit_name, amount, 'EMERGENCY CREDIT') as amount_sos_credit, \
    _getTagValueUDF(contact_channel_id, benefit_name, amount, 'EMERGENCY DATA') as amount_sos_data, _getTagValueUDF(contact_channel_id, benefit_name, amount, '') as amount_other_recharge, \
    _getTagValueUDF(contact_channel_id, benefit_name, initial_loan_amount, 'EMERGENCY CREDIT') as initial_sos_credit_loan_amount, _getTagValueUDF(contact_channel_id, benefit_name, initial_loan_amount, 'EMERGENCY DATA') as initial_sos_data_loan_amount, _getTagValueUDF(contact_channel_id, benefit_name, initial_loan_amount, '') as initial_other_recharge_loan_amount, \
    _getTagValueUDF(contact_channel_id, benefit_name, amount_already_pay_back, 'EMERGENCY CREDIT') as already_pay_back_sos_credit, _getTagValueUDF(contact_channel_id, benefit_name, amount_already_pay_back, 'EMERGENCY DATA') as already_pay_back_sos_data, _getTagValueUDF(contact_channel_id, benefit_name, amount_already_pay_back, '') as already_pay_back_other_recharge, \
    _getTagValueUDF(contact_channel_id, benefit_name, amount_remain_to_pay_back, 'EMERGENCY CREDIT') as remain_to_pay_back_sos_credit, _getTagValueUDF(contact_channel_id, benefit_name, amount_remain_to_pay_back, 'EMERGENCY DATA') as remain_to_pay_back_sos_data, _getTagValueUDF(contact_channel_id, benefit_name, amount_remain_to_pay_back, '') as remain_to_pay_back_other_recharge from td_df_filtered""")

    # Grouping the data for stats
    td_df = td_df_oba_exploded.na.fill(value=0.0).fillna("").groupby(["msisdn", "subscriber_type"]).agg(count('amount_sos_credit').alias("nbr sos_credit"),\
                                                                                                        avg('amount_sos_credit').alias("avg amount_sos_credit"),\
                                                                                                        stddev('amount_sos_credit').alias("std_dev amount_sos_credit"),\
                                                                                                        expr("percentile(amount_sos_credit, 0.00)").alias("0% amount_sos_credit"),\
                                                                                                        expr("percentile(amount_sos_credit, 0.25)").alias("25% amount_sos_credit"),\
                                                                                                        expr("percentile(amount_sos_credit, 0.50)").alias("50% amount_sos_credit"),\
                                                                                                        expr("percentile(amount_sos_credit, 0.75)").alias("75% amount_sos_credit"),\
                                                                                                        expr("percentile(amount_sos_credit, 1.00)").alias("100% amount_sos_credit"),\
                                                                                                        avg('initial_sos_credit_loan_amount').alias("avg initial_sos_credit_loan_amount"),\
                                                                                                        stddev('initial_sos_credit_loan_amount').alias("std_dev initial_sos_credit_loan_amount"),\
                                                                                                        expr("percentile(initial_sos_credit_loan_amount, 0.00)").alias("0% initial_sos_credit_loan_amount"),\
                                                                                                        expr("percentile(initial_sos_credit_loan_amount, 0.25)").alias("25% initial_sos_credit_loan_amount"),\
                                                                                                        expr("percentile(initial_sos_credit_loan_amount, 0.50)").alias("50% initial_sos_credit_loan_amount"),\
                                                                                                        expr("percentile(initial_sos_credit_loan_amount, 0.75)").alias("75% initial_sos_credit_loan_amount"),\
                                                                                                        expr("percentile(initial_sos_credit_loan_amount, 1.00)").alias("100% initial_sos_credit_loan_amount"),\
                                                                                                        avg('already_pay_back_sos_credit').alias("avg already_pay_back_sos_credit"),\
                                                                                                        stddev('already_pay_back_sos_credit').alias("std_dev already_pay_back_sos_credit"),\
                                                                                                        expr("percentile(already_pay_back_sos_credit, 0.00)").alias("0% already_pay_back_sos_credit"),\
                                                                                                        expr("percentile(already_pay_back_sos_credit, 0.25)").alias("25% already_pay_back_sos_credit"),\
                                                                                                        expr("percentile(already_pay_back_sos_credit, 0.50)").alias("50% already_pay_back_sos_credit"),\
                                                                                                        expr("percentile(already_pay_back_sos_credit, 0.75)").alias("75% already_pay_back_sos_credit"),\
                                                                                                        expr("percentile(already_pay_back_sos_credit, 1.00)").alias("100% already_pay_back_sos_credit"), \
                                                                                                        avg('remain_to_pay_back_sos_credit').alias("avg remain_to_pay_back_sos_credit"), \
                                                                                                        stddev('remain_to_pay_back_sos_credit').alias("std_dev remain_to_pay_back_sos_credit"), \
                                                                                                        expr("percentile(remain_to_pay_back_sos_credit, 0.00)").alias("0% remain_to_pay_back_sos_credit"), \
                                                                                                        expr("percentile(remain_to_pay_back_sos_credit, 0.25)").alias("25% remain_to_pay_back_sos_credit"), \
                                                                                                        expr("percentile(remain_to_pay_back_sos_credit, 0.50)").alias("50% remain_to_pay_back_sos_credit"), \
                                                                                                        expr("percentile(remain_to_pay_back_sos_credit, 0.75)").alias("75% remain_to_pay_back_sos_credit"), \
                                                                                                        expr("percentile(remain_to_pay_back_sos_credit, 1.00)").alias("100% remain_to_pay_back_sos_credit"), \
                                                                                                       count('amount_sos_data').alias("nbr sos_data"), \
                                                                                                       avg('amount_sos_data').alias("avg amount_sos_data"), \
                                                                                                       stddev('amount_sos_data').alias("std_dev amount_sos_data"), \
                                                                                                       expr("percentile(amount_sos_data, 0.00)").alias("0% amount_sos_data"), \
                                                                                                       expr("percentile(amount_sos_data, 0.25)").alias("25% amount_sos_data"), \
                                                                                                       expr("percentile(amount_sos_data, 0.50)").alias("50% amount_sos_data"), \
                                                                                                       expr("percentile(amount_sos_data, 0.75)").alias("75% amount_sos_data"), \
                                                                                                       expr("percentile(amount_sos_data, 1.00)").alias("100% amount_sos_data"), \
                                                                                                       avg('initial_sos_data_loan_amount').alias("avg initial_sos_data_loan_amount"), \
                                                                                                       stddev('initial_sos_data_loan_amount').alias("std_dev initial_sos_data_loan_amount"), \
                                                                                                       expr("percentile(initial_sos_data_loan_amount, 0.00)").alias("0% initial_sos_data_loan_amount"), \
                                                                                                       expr("percentile(initial_sos_data_loan_amount, 0.25)").alias("25% initial_sos_data_loan_amount"), \
                                                                                                       expr("percentile(initial_sos_data_loan_amount, 0.50)").alias("50% initial_sos_data_loan_amount"), \
                                                                                                       expr("percentile(initial_sos_data_loan_amount, 0.75)").alias("75% initial_sos_data_loan_amount"), \
                                                                                                       expr("percentile(initial_sos_data_loan_amount, 1.00)").alias("100% initial_sos_data_loan_amount"), \
                                                                                                       avg('already_pay_back_sos_data').alias("avg already_pay_back_sos_data"), \
                                                                                                       stddev('already_pay_back_sos_data').alias("std_dev already_pay_back_sos_data"), \
                                                                                                       expr("percentile(already_pay_back_sos_data, 0.00)").alias("0% already_pay_back_sos_data"), \
                                                                                                       expr("percentile(already_pay_back_sos_data, 0.25)").alias("25% already_pay_back_sos_data"), \
                                                                                                       expr("percentile(already_pay_back_sos_data, 0.50)").alias("50% already_pay_back_sos_data"), \
                                                                                                       expr("percentile(already_pay_back_sos_data, 0.75)").alias("75% already_pay_back_sos_data"), \
                                                                                                       expr("percentile(already_pay_back_sos_data, 1.00)").alias("100% already_pay_back_sos_data"), \
                                                                                                       avg('remain_to_pay_back_sos_data').alias("avg remain_to_pay_back_sos_data"), \
                                                                                                       stddev('remain_to_pay_back_sos_data').alias("std_dev remain_to_pay_back_sos_data"), \
                                                                                                       expr("percentile(remain_to_pay_back_sos_data, 0.00)").alias("0% remain_to_pay_back_sos_data"), \
                                                                                                       expr("percentile(remain_to_pay_back_sos_data, 0.25)").alias("25% remain_to_pay_back_sos_data"), \
                                                                                                       expr("percentile(remain_to_pay_back_sos_data, 0.50)").alias("50% remain_to_pay_back_sos_data"), \
                                                                                                       expr("percentile(remain_to_pay_back_sos_data, 0.75)").alias("75% remain_to_pay_back_sos_data"), \
                                                                                                       expr("percentile(remain_to_pay_back_sos_data, 1.00)").alias("100% remain_to_pay_back_sos_data"), \
                                                                                                       count('amount_other_recharge').alias("nbr other_recharge"), \
                                                                                                       avg('amount_other_recharge').alias("avg amount_other_recharge"), \
                                                                                                       stddev('amount_other_recharge').alias("std_dev amount_other_recharge"), \
                                                                                                       expr("percentile(amount_other_recharge, 0.00)").alias("0% amount_other_recharge"), \
                                                                                                       expr("percentile(amount_other_recharge, 0.25)").alias("25% amount_other_recharge"), \
                                                                                                       expr("percentile(amount_other_recharge, 0.50)").alias("50% amount_other_recharge"), \
                                                                                                       expr("percentile(amount_other_recharge, 0.75)").alias("75% amount_other_recharge"), \
                                                                                                       expr("percentile(amount_other_recharge, 1.00)").alias("100% amount_other_recharge"), \
                                                                                                       avg('initial_other_recharge_loan_amount').alias("avg initial_other_recharge_loan_amount"), \
                                                                                                       stddev('initial_other_recharge_loan_amount').alias("std_dev initial_other_recharge_loan_amount"), \
                                                                                                       expr("percentile(initial_other_recharge_loan_amount, 0.00)").alias("0% initial_other_recharge_loan_amount"), \
                                                                                                       expr("percentile(initial_other_recharge_loan_amount, 0.25)").alias("25% initial_other_recharge_loan_amount"), \
                                                                                                       expr("percentile(initial_other_recharge_loan_amount, 0.50)").alias("50% initial_other_recharge_loan_amount"), \
                                                                                                       expr("percentile(initial_other_recharge_loan_amount, 0.75)").alias("75% initial_other_recharge_loan_amount"), \
                                                                                                       expr("percentile(initial_other_recharge_loan_amount, 1.00)").alias("100% initial_other_recharge_loan_amount"), \
                                                                                                       avg('already_pay_back_other_recharge').alias("avg already_pay_back_other_recharge"), \
                                                                                                       stddev('already_pay_back_other_recharge').alias("std_dev already_pay_back_other_recharge"), \
                                                                                                       expr("percentile(already_pay_back_other_recharge, 0.00)").alias("0% already_pay_back_other_recharge"), \
                                                                                                       expr("percentile(already_pay_back_other_recharge, 0.25)").alias("25% already_pay_back_other_recharge"), \
                                                                                                       expr("percentile(already_pay_back_other_recharge, 0.50)").alias("50% already_pay_back_other_recharge"), \
                                                                                                       expr("percentile(already_pay_back_other_recharge, 0.75)").alias("75% already_pay_back_other_recharge"), \
                                                                                                       expr("percentile(already_pay_back_other_recharge, 1.00)").alias("100% already_pay_back_other_recharge"), \
                                                                                                       avg('remain_to_pay_back_other_recharge').alias("avg remain_to_pay_back_other_recharge"), \
                                                                                                       stddev('remain_to_pay_back_other_recharge').alias("std_dev remain_to_pay_back_other_recharge"), \
                                                                                                       expr("percentile(remain_to_pay_back_other_recharge, 0.00)").alias("0% remain_to_pay_back_other_recharge"), \
                                                                                                       expr("percentile(remain_to_pay_back_other_recharge, 0.25)").alias("25% remain_to_pay_back_other_recharge"), \
                                                                                                       expr("percentile(remain_to_pay_back_other_recharge, 0.50)").alias("50% remain_to_pay_back_other_recharge"), \
                                                                                                       expr("percentile(remain_to_pay_back_other_recharge, 0.75)").alias("75% remain_to_pay_back_other_recharge"), \
                                                                                                       expr("percentile(remain_to_pay_back_other_recharge, 1.00)").alias("100% remain_to_pay_back_other_recharge"))

    td_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_to_" + end_date + "_results.csv"
    extract_recharge_data(spark, td_driver, td_url, td_user, td_password, start_date, end_date, filename)

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
    destination_folder = "/home/davidtia/incoming/telco_data/activity_detail/recharge/"
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_20210930.zip"
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
    app_name = "PySpark OBA data extraction om"
    driver_home = "/home/patrice.nzi/backup/oba/jars"
    td_driver = 'com.teradata.jdbc.TeraDriver'
    td_url = "jdbc:teradata://10.241.10.4/Database=GANDHI,LOGMECH=TD2"
    td_user = "pgan"
    td_password = "ocit"

    spark = SparkSession.builder.config("spark.executor.memory", "8g").\
        config("spark.driver.memory", "2g").\
        config("spark.memory.offHeap.enabled", True).\
        config("spark.memory.offHeap.size", "2g").\
        config("spark.jars", driver_home + "/terajdbc4.jar").\
        appName(app_name).getOrCreate()
  
    # registering the udfs
    register_utils_udfs(spark)

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
        prefix = "oba_recharge_aggregation_newclients_of_" + args.edate + "_"
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

        prefix = "oba_recharge_aggregation_"
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)

    spark.stop()


if __name__=="__main__":
    main()
