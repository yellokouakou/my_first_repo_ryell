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
from dates_generator import generate_previous_week_date


def extract_ftth_data(spark, oracle_driver, oracle_url, oracle_user, oracle_pwd, a_date, output_path):
    oracle_process = ExtractorSQL(spark, True, oracle_driver, oracle_url, oracle_user, oracle_pwd)
    ftth_sql = "select plan_tarifaire, debit, categorie, categorie_client, etat, contact, second_contact, ville, nd as numero from dwhdev.bub_fixe_parc_ods_ftth where date_jour='" + a_date + "'"
    oracle_process.load(ftth_sql)
    oracle_process.to_table("oracle_ftth")

    # Normalizing the data
    oracle_df_oba_norm = spark.sql("""select plan_tarifaire, debit, categorie, categorie_client, etat, case when rlike(contact,'^(225)[0456789]{1}[0-3]{1}[0-9]*$') then '22501'||right(contact,8)
            when rlike(contact,'^(225)[0456789]{1}[4-6]{1}[0-9]*$') then '22505'||right(contact,8)
            when rlike(contact,'^(225)[0456789]{1}[7-9]{1}[0-9]*$') then '22507'||right(contact,8)
            when rlike(contact,'^[0456789]{1}[0-3]{1}[0-9]*$') then '22501'||right(contact,8)
            when rlike(contact,'^[0456789]{1}[4-6]{1}[0-9]*$') then '22505'||right(contact,8)
            when rlike(contact,'^[0456789]{1}[7-9]{1}[0-9]*$') then '22507'||right(contact,8)
            when rlike(contact,'^(225)[0-9]*$') then contact
        end msisdn, case when rlike(second_contact,'^(225)[0456789]{1}[0-3]{1}[0-9]*$') then '22501'||right(second_contact,8)
            when rlike(second_contact,'^(225)[0456789]{1}[4-6]{1}[0-9]*$') then '22505'||right(second_contact,8)
            when rlike(second_contact,'^(225)[0456789]{1}[7-9]{1}[0-9]*$') then '22507'||right(second_contact,8)
            when rlike(second_contact,'^[0456789]{1}[0-3]{1}[0-9]*$') then '22501'||right(second_contact,8)
            when rlike(second_contact,'^[0456789]{1}[4-6]{1}[0-9]*$') then '22505'||right(second_contact,8)
            when rlike(second_contact,'^[0456789]{1}[7-9]{1}[0-9]*$') then '22507'||right(second_contact,8)
            when rlike(second_contact,'^(225)[0-9]*$') then second_contact
        end second_msisdn, ville, numero from oracle_ftth""")
    oracle_df_oba_norm.createOrReplaceTempView("oracle_ftth_norm")

    # exploding the data with msisdn and second_msisdn
    oracle_df_oba_exploded = spark.sql("""(select plan_tarifaire, debit, categorie, categorie_client, etat, msisdn, ville, numero from oracle_ftth_norm) union all (select plan_tarifaire, debit, categorie, categorie_client, etat, second_msisdn as msisdn, ville, numero from oracle_ftth_norm)""")
    oracle_df_oba_norm.createOrReplaceTempView("oracle_ftth_norm")

    # Filtering out oba clients data
    oracle_df = spark.sql("""select msisdn, plan_tarifaire, debit, categorie, categorie_client, etat, ville, numero from oracle_ftth_norm inner join oba_clients on oracle_ftth_norm.msisdn = oba_clients.Phone""")

    oracle_df.toPandas().to_csv(output_path, sep=",", index=False)


def main():
    # Defining the value of some variables
    host = '172.16.101.205'
    keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABgQC7IomqG+usVr+Cy0HJ/h47q6jxJ8A+poAAv5Eo4jH2yeTxgusXAt4FosjPKchxHCADNus7uNBOYNcQtiznOwAmhWIdFEWAO8dubJooyhhz+0+51VSID7THhfjvkBJDroRimwWd16mPXq8nA45JfxPSx5z7qWVOqz7B4aMXaOeODFEU/QJ44+8o3cqjdXsVqTgOvH2whKmvPx837vkjidSi5QJEM53B28OUzpMn9KHa6Hx4rYQtjKEPKbmNfnCooPtzpM4cZQXHd3H3Ga4Wln3LXbVvP2eQAKdXwqhxfJ8u9XO5kHOnGN8Iv25BuTvQ5YkMH/wtu26YWJUCb+M0bJl3Gjuq/zAmUrGxEmejl+fqkbiHCPcRK9FA9x4PKoONPqcT3RJ4SSKbyDGrjYKDICxgx3NTkg7l1Hs4RRphw5yGuXQYEOcb2PW+VnecoBB5RbE7S3NLoQdRV+O9v6ISmpDkqvpSfZahfdD8T2AJKwy1Nvu6oCZOIJNdvILO4KwksJM="""
    port = 22
    user = "davidtia"
    password = "********"
    destination_folder = "/home/davidtia/incoming/recurrence/ftth/" # historic data in telco_data folder.
    source_client_file = "/home/davidtia/output/AllCustomers_OBA_CI_old_last.csv"
    previous_date = generate_previous_week_date()
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
    parser.add_argument('--date', dest='date', default=previous_date, help="start date for data")
    parser.add_argument('--auto', dest='auto', default=auto, help="generation automatique des dates ?")

    args = parser.parse_args()

    # ------------------------------------------
    # create an instance of SparkSession object
    # ------------------------------------------
    app_name = "PySpark OBA data extraction om"
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

    # result file prefix
    a_date = args.date

    if args.auto == "true":
        a_date = previous_date
        
    filename = "OBA_DATA_FTTH_" + a_date + "_RESULTS.csv"
    extract_ftth_data(spark, oracle_driver, oracle_url, oracle_user, oracle_password, a_date, filename)

    # copying the data to server
    remote_path = args.dest + filename

    # SSH connection
    # fast_ssh = FastSshCopy(args.host, args.port, args.user, args.password)
    # fast_ssh.connect()
    # fast_ssh.send(filename, remote_path)

    print("file: "+ filename + " has been copied to oba.")
    remove(filename)
    print("file: "+ filename + " has been removed.")

    spark.stop()


if __name__=="__main__":
    main()

