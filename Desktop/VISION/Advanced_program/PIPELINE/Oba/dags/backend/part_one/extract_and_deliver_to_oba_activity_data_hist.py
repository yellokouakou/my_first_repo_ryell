import argparse
from os.path import join
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, stddev, expr
from os import path, remove

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_dates_from_weeks, generate_previous_week


def extract_activity_data(spark, td_driver, td_url, td_user, td_password, beg_date, end_date, output_path):
    td_process = ExtractorSQL(spark, False, td_driver, td_url, td_user, td_password)
    td_sql = "SELECT * FROM IGANB_DWH.OBA_ACTIVITY_DATA_PNN WHERE CAST(DATEDATA AS DATE FORMAT 'yyyyMMdd') BETWEEN CAST('" + beg_date + "'  AS date FORMAT 'yyyyMMdd') AND CAST('" + end_date + "'  AS date FORMAT 'yyyyMMdd')"
    td_process.load(td_sql)
    td_process.to_table("td_oba_activity")

   # Normalizing the data
    td_df_oba_norm = spark.sql("""select concat("22507", right(mmid,8)) as mmid, datedata, balombeg, balomend, nbcashin, volcashin, nbcashout, volcashout, nbcharge, nbp2pin, volp2pin, nbp2pout, volp2pout, nbmerpay, volmerpay, nbbillpay, volbillpay, nbotherin, volotherin, nbotherout, volotherout, nbp2pphonein, nbp2pphoneout, nbtotalcharge, voltotalcharge, nbcall, timecall, nbsms, nbcontactcallin, nbcontactcallout, nbcontactsms, geoloccomm, geolocnight, preferedagent, preferedagentqual, mmdate, devicetype, group_id from td_oba_activity""")
    td_df_oba_norm.createOrReplaceTempView("td_df_oba_activity_norm")
    
    # Filtering out oba clients data
    td_df = spark.sql("""select mmid, datedata, balombeg, balomend, nbcashin, volcashin, nbcashout, volcashout, nbcharge, nbp2pin, volp2pin, nbp2pout, volp2pout, nbmerpay, volmerpay, nbbillpay, volbillpay, nbotherin, volotherin, nbotherout, volotherout, nbp2pphonein, nbp2pphoneout, nbtotalcharge, voltotalcharge, nbcall, timecall, nbsms, nbcontactcallin, nbcontactcallout, nbcontactsms, geoloccomm, geolocnight, preferedagent, preferedagentqual, mmdate, devicetype, group_id from td_df_oba_activity_norm inner join oba_clients on td_df_oba_activity_norm.mmid = oba_clients.Phone""")

    td_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_to_" + end_date + "_results.csv"
    extract_activity_data(spark, td_driver, td_url, td_user, td_password, start_date, end_date, filename)

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
    destination_folder = "/home/davidtia/incoming/recurrence/activitydata/"
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_old_last.csv"
    # beg_date, end_date = generate_previous_week()
    beg_date = "20200824"
    end_date = "20200830"
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
    parser.add_argument('--auto', dest='auto', default=auto, help="should we automatically generate the date ?")

    args = parser.parse_args()

    # ------------------------------------------
    # create an instance of SparkSession object
    # ------------------------------------------
    app_name = "PySpark OBA data extraction om"
    driver_home = "/home/patrice.nzi/backup/oba/airflow_prod/dags/jars/"
    td_driver = 'com.teradata.jdbc.TeraDriver'
    td_url = "jdbc:teradata://10.241.10.7/Database=IGANB_DWH,LOGMECH=TD2"
    td_user = "igan"
    td_password = "ocit"

    spark = SparkSession.builder.config("spark.executor.memory", "8g").\
        config("spark.driver.memory", "2g").\
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
    dates = generate_dates_from_weeks(26, args.edate)
    prefix = "activity_data_transactions_" 
    for pair_of_dates in dates:
        sdate = pair_of_dates[0]
        edate = pair_of_dates[1]
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)
        
    spark.stop()


if __name__=="__main__":
    main()
