import argparse
from os.path import join
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from os import remove

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_dates_from_weeks


def extract_paiement_facture_data(spark, oracle_driver, oracle_url, oracle_user, oracle_pwd, beg_date, end_date, output_path):
    oracle_process = ExtractorSQL(spark, True, oracle_driver, oracle_url, oracle_user, oracle_pwd)
    facture_sql_nocanal = "SELECT TRUNC(T1.TRANSACTION_DATE_TIME) as PERIOD, \
T1.SENDER_MSISDN as MSISDN, \
T2.PARTENAIRES as TYPE_FACTURE, \
COUNT(*) as NB_TRX, \
SUM(T1.TRANSACTION_AMOUNT) as MTT_TRX \
FROM DWHDEV.TANGO_ODS_APGL T1, DWHDEV.TANGO_PARAM_NEW_ADDONS T2 \
WHERE T1.RECEIVER_MSISDN = T2.MSISDN \
AND T1.TRANSACTION_STATUS='TS' \
AND T2.VAS_ID='FACTURES' \
AND T2.OFFER_NAME !='Financier' \
AND TRUNC(T2.END_DATE)=TO_DATE('31/12/9999','DD/MM/YYYY') \
AND T1.RECEIVER_CATEGORY_CODE!='PDVCAPAY' \
AND T1.RECEIVER_DOMAIN_CODE!='CAPAY' \
AND T1.TRANSACTION_DATE_TIME BETWEEN TO_DATE('" + beg_date +"', 'yyyy-MM-dd') AND TO_DATE('" + end_date + "','yyyy-MM-dd') \
GROUP BY TRUNC(T1.TRANSACTION_DATE_TIME), T1.SENDER_MSISDN, T2.PARTENAIRES"

    facture_sql_canal = "SELECT TRUNC(T1.TRANSACTION_DATE_TIME) as PERIOD, \
T1.SENDER_MSISDN as MSISDN, \
'CANAL+' as TYPE_FACTURE, \
COUNT(*) as NB_TRX, \
SUM(T1.TRANSACTION_AMOUNT) as MTT_TRX \
FROM DWHDEV.TANGO_ODS_APGL T1 \
WHERE T1.TRANSACTION_STATUS='TS' \
AND T1.COMMISSION_OCA>0 \
AND RECEIVER_CATEGORY_CODE='PDVCAPAY' \
AND RECEIVER_DOMAIN_CODE='CAPAY' \
AND T1.TRANSACTION_DATE_TIME BETWEEN TO_DATE('" + beg_date +"', 'yyyy-MM-dd') AND TO_DATE('" + end_date + "','yyyy-MM-dd') \
GROUP BY TRUNC(T1.TRANSACTION_DATE_TIME), T1.SENDER_MSISDN, 'CANAL+'"

    facture_sql_assurances = "SELECT TRUNC(T1.TRANSACTION_DATE_TIME) as PERIOD, \
T1.SENDER_MSISDN as MSISDN, \
'PAIEMENT ASSURANCE' as TYPE_FACTURE, \
COUNT(*) as NB_TRX, \
SUM(T1.TRANSACTION_AMOUNT) as MTT_TRX \
FROM DWHDEV.TANGO_ODS_APGL T1 \
WHERE EXISTS (SELECT * FROM DWHDEV.TANGO_PARAM_NEW_ADDONS T2 WHERE T1.RECEIVER_MSISDN = T2.MSISDN AND T2.VAS_ID='FACTURES' AND T2.OFFER_NAME ='Financier' AND TRUNC(T2.END_DATE)=TO_DATE('31/12/9999','DD/MM/YYYY')) \
AND T1.TRANSACTION_STATUS='TS' \
AND T1.TRANSACTION_DATE_TIME BETWEEN TO_DATE('" + beg_date +"', 'yyyy-MM-dd') AND TO_DATE('" + end_date + "','yyyy-MM-dd') \
GROUP BY TRUNC(T1.TRANSACTION_DATE_TIME), T1.SENDER_MSISDN, 'PAIEMENT ASSURANCE'"

    oracle_process.load(facture_sql_canal)
    oracle_process.to_table("oracle_facture_canal")

    oracle_process.load(facture_sql_nocanal)
    oracle_process.to_table("oracle_facture_nocanal")

    oracle_process.load(facture_sql_assurances)
    oracle_process.to_table("oracle_facture_assurance")

    # aggregating
    oracle_df_facture = spark.sql("""select * from oracle_facture_assurance""")
    oracle_df = spark.sql("""select * from oracle_facture_canal""")
    oracle_df_facture = oracle_df_facture.union(oracle_df)
    
    oracle_df_nocan = spark.sql("""select * from oracle_facture_nocanal""")
    oracle_df_facture = oracle_df_facture.union(oracle_df_nocan)

    oracle_df_facture.show()
    oracle_df_facture.createOrReplaceTempView("oracle_facture")

    # Normalizing the data
    df_oba_norm = spark.sql("""select period, concat("22507", right(msisdn,8)) as msisdn, type_facture, nb_trx, mtt_trx from oracle_facture""")
    df_oba_norm.createOrReplaceTempView("oracle_facture_norm")

    # Filtering out oba clients data
    oracle_df = spark.sql("""select period, msisdn, type_facture, nb_trx, mtt_trx from oracle_facture_norm inner join oba_clients on oracle_facture_norm.msisdn = oba_clients.Phone""")

    oracle_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_TO_" + end_date + ".csv"
    extract_paiement_facture_data(spark, oracle_driver, oracle_url, oracle_user, oracle_password, start_date, end_date, filename)

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
    destination_folder = "/home/davidtia/incoming/omci/"
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
    app_name = "PySpark OBA data extraction facture om"
    driver_home = "/home/patrice.nzi/backup/oba/jars"
    oracle_driver = 'oracle.jdbc.OracleDriver'
    oracle_url = "jdbc:oracle:thin:@10.242.79.49:1521:DWOCIT"
    oracle_user = "dwh"
    oracle_password = "dwh"

    spark = SparkSession.builder.config("spark.executor.memory", "8g").\
        config("spark.driver.memory", "2g").\
        config("spark.memory.offHeap.enabled", True).\
        config("spark.memory.offHeap.size", "2g").\
        config("spark.jars", driver_home + "/ojdbc7.jar").\
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
        prefix = "TRX_PAIEMENT_FACTURE_OBA_CUSTOMERS_NEWCLIENTS_OF_" + args.edate + "_"
        for pair_of_dates in dates:
            sdate = pair_of_dates[0]
            edate = pair_of_dates[1]
            extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, sdate, edate, prefix)
    else:
        sdate = args.sdate
        edate = args.edate

        if args.auto == "true":
            sdate = beg_date
            edate = end_date

        prefix = "TRX_PAIEMENT_FACTURE_OBA_CUSTOMERS_"
        extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, sdate, edate, prefix)
    
    spark.stop()


if __name__=="__main__":
    main()
