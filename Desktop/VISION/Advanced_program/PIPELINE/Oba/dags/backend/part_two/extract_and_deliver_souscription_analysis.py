import argparse
from os.path import join
from pathlib import Path
import pandas as pd
import re
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, stddev, expr, sum as _sum

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


def get_category_value(benefit, category):
    benefit = str(benefit)
    benefit = benefit.upper()
    datas_reg = ('DATA', 'MB', 'GB')
    calls_sms = ('MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH')
    has_data = any(it in benefit for it in datas_reg)
    has_calls_sms = any(it in benefit for it in calls_sms) or re.findall('[0-9]+', benefit)
    if has_data and has_calls_sms:
        if category == "PASS_MIX":
            return 1.0
        else:
            return 0.0
    if has_data:
        if category == "PASS_DATA":
            return 1.0
        else:
            return 0.0
    if has_calls_sms:
        if category == "PASS_APPEL":
            return 1.0
        else:
            return 0.0
    if category == "OTHER_PASS":
        return 1.0
    else:
        return 0.0


def get_category_value_mix(benefit):
    return get_category_value(benefit, 'PASS_MIX')

def get_category_value_data(benefit):
    return get_category_value(benefit, 'PASS_DATA')

def get_category_value_appel(benefit):
    return get_category_value(benefit, 'PASS_APPEL')

def get_category_value_other(benefit):
    return get_category_value(benefit, 'OTHER_PASS')

def register_utils_udfs(spark):
    spark.udf.register("_prettifySubsUDF", transform_subs, StringType())
    spark.udf.register("_getCategoryValueMixUDF", get_category_value_mix, FloatType())
    spark.udf.register("_getCategoryValueDataUDF", get_category_value_data, FloatType())
    spark.udf.register("_getCategoryValueAppelUDF", get_category_value_appel, FloatType())
    spark.udf.register("_getCategoryValueOtherUDF", get_category_value_other, FloatType())


def extract_souscription_data(spark, td_driver, td_url, td_user, td_password, beg_date, end_date, output_path):
    td_process = ExtractorSQL(spark, False, td_driver, td_url, td_user, td_password)
    td_subscription_sql = "SELECT ACC_NBR AS MSISDN, BENEFIT_NAME, SUBSCRIBER_TYPE_LV from PGANV_DWH.SUBSCRIPTION_ALL WHERE CAST(CREATED_DATE AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "'  AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "'  AS date FORMAT 'yyyyMMdd')"
    td_process.load(td_subscription_sql)
    td_process.to_table("td_subscription")

    # Normalizing the data
    td_df_oba_norm = spark.sql("""select concat("22507", right(msisdn,8)) as msisdn, benefit_name, subscriber_type_lv from td_subscription""")
    td_df_oba_norm.createOrReplaceTempView("td_df_oba_norm")

    # Filtering out oba clients data
    td_df_filtered = spark.sql("""select msisdn, benefit_name, subscriber_type_lv from td_df_oba_norm inner join oba_clients on td_df_oba_norm.msisdn = oba_clients.Phone""")
    td_df_filtered.createOrReplaceTempView("td_df_oba_filtered")

    # applying the needed transformations on oba clients data
    td_df_tr = spark.sql("""select msisdn, _prettifySubsUDF(subscriber_type_lv) as subscriber_type, _getCategoryValueMixUDF(benefit_name) as mix_sub, \
    _getCategoryValueDataUDF(benefit_name) as data_sub, _getCategoryValueAppelUDF(benefit_name) as appel_sms_sub, _getCategoryValueOtherUDF(benefit_name) as others_sub from td_df_oba_filtered""")
    td_df_tr.show()

    # Grouping the data for stats
    td_df = td_df_tr.groupby(["msisdn", "subscriber_type"]).agg(_sum('data_sub').alias("nbr_souscription_data"),\
    _sum('mix_sub').alias("nbr_souscription_mix"), _sum('appel_sms_sub').alias("nbr_souscription_appel_sms"), _sum('others_sub').alias("nbr_souscription_others"))

    td_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_to_" + end_date + "_results.csv"
    extract_souscription_data(spark, td_driver, td_url, td_user, td_password, start_date, end_date, filename)

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
    destination_folder = "/home/davidtia/incoming/telco_data/activity_detail/souscription/"
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
    app_name = "PySpark OBA data extraction souscription"
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
        prefix = "oba_subscription_aggregation_newclients_of_" + args.edate + "_"
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

        prefix = "oba_subscription_aggregation_"
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)

    spark.stop()


if __name__=="__main__":
    main()
