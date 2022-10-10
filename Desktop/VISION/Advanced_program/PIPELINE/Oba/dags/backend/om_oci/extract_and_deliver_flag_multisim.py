import argparse
from os.path import join
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, stddev, expr, col, sum as _sum
from os import remove
from datetime import date

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week


def extract_multisim_data(spark, oracle_driver, oracle_url, oracle_user, oracle_pwd, output_path):
    oracle_process = ExtractorSQL(spark, True, oracle_driver, oracle_url, oracle_user, oracle_pwd)
    multisim_sql = "select msisdn, max(nvl(statut_orange,0)) statut_msim_orange, max(nvl(STATUT_MOOV,0)) STATUT_MSIM_MOOV, max(nvl(STATUT_MTN,0))  STATUT_MSIM_MTN from \
( \
select case when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)\d{10}$') then msisdn\
        end msisdn,\
       statut_orange,\
       STATUT_MOOV,\
       STATUT_MTN \
from msim_inbox_ref \
UNION ALL \
select case when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)\d{10}$') then msisdn\
        end msisdn,\
        max(case when top_ds_on=1 then 1 else 0 end) statut_orange,\
        max(case when op_offnet='MOOV' then 1 else 0 end) statut_moov,\
        max(case when op_offnet='MTN' then 1 else 0 end) statut_mtn \
from msim_ref_20210701_decrypte where \
        case when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)\d{10}$') then msisdn\
        end is not null \
group by \
        case when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[0-3]{1}\d{6}$') then '22501'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[4-6]{1}\d{6}$') then '22505'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^[0456789]{1}[7-9]{1}\d{6}$') then '22507'||substr(msisdn,4,8)\
            when REGEXP_LIKE(msisdn,'^(225)\d{10}$') then msisdn\
        end) group by msisdn"
    
    oracle_process.load(multisim_sql)
    oracle_process.to_table("oracle_multi_sim")

    # Filtering out oba clients data
    oracle_df_filtered = spark.sql("""select msisdn, statut_msim_orange, statut_msim_moov, statut_msim_mtn from oracle_multi_sim inner join oba_clients on oracle_multi_sim.msisdn = oba_clients.Phone""")
    oracle_df_filtered.show()

    oracle_df = oracle_df_filtered.groupby("msisdn").agg(_sum('statut_msim_orange').alias("statut_msim_orange"), \
    _sum('statut_msim_moov').alias("statut_msim_moov"), \
    _sum('statut_msim_mtn').alias("statut_msim_mtn"))

    oracle_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, prefix):

    # result file prefix
    filename = prefix + ".csv"
    extract_multisim_data(spark, oracle_driver, oracle_url, oracle_user, oracle_password, filename)

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
    destination_folder = "/home/davidtia/incoming/telco_data/"
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_20210930.zip"
    beg_date, end_date = generate_previous_week()
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
    parser.add_argument('--auto', dest='auto', default="false", help="automatic date generation")

    args = parser.parse_args()

    # ------------------------------------------
    # create an instance of SparkSession object
    # ------------------------------------------
    app_name = "PySpark OBA data extraction om"
    driver_home = "/home/patrice.nzi/backup/oba/jars"
    oracle_driver = 'oracle.jdbc.OracleDriver'
    oracle_url = "jdbc:oracle:thin:@10.242.69.235:1521:PRADAOCIT"
    oracle_user = "miner_user"
    oracle_password = "miner_user"

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

    today = date.today()
    today_date = today.strftime("%d%m%Y")
    
    # check if it for new clients
    if str(args.clients).find("new") != -1: # contains new clients
        prefix = "OBA_DATA_MSIM_NEWCLIENTS_OF_" + today_date + "_FOR_"+ args.sdate + "_TO_" + args.edate
        extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, prefix)
    else:
        sdate = args.sdate
        edate = args.edate

        if args.auto == "true":
            sdate = beg_date
            edate = end_date

        prefix = "OBA_DATA_MSIM_OF_" + today_date + "_FOR_"+ sdate + "_TO_" + edate
        extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, prefix)

    spark.stop()


if __name__=="__main__":
    main()  
