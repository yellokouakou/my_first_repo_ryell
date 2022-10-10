import argparse
from create_telco_graph import TelcoGraph, Category, SVC
from pyspark.sql import SparkSession
from graphframes import *
from graphframes.lib import AggregateMessages as AM
from pyspark.sql.functions import avg, count, stddev, min as sqlmin, max as sqlmax, sum as sqlsum
import pandas as pd
from os import remove
from os.path import join
from pathlib import Path

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))


from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_dates_from_weeks


def extract_graph_telco_data(spark, td_driver, td_url, td_user, td_password, re_id, categorie, svc, beg_date, end_date, output_path):
    Graph = TelcoGraph(spark, False, td_driver, td_url, td_user, td_password, beg_date, end_date, "data_telco", "msisdn")
    g = Graph.build_telco_graphs(re_id, svc, categorie)
    oba_clients = spark.sql("select Phone as id from oba_clients")
    data = oba_clients.withColumnRenamed("Phone", "id")

    # svc in sva, ims, unknown, data
    if svc in [SVC.DATA, SVC.SVA, SVC.IMS, SVC.UNKNOWN]:
        req = "select * from data_telco_" + str(svc.name).lower()
        data = spark.sql(req)
    elif svc == SVC.SMS:
        data_sms = g.aggregateMessages(count(AM.msg).alias("nbr_users_exch_sms_out"), sendToSrc=AM.edge["nbr_exch_sms"])
        aggOutMin = g.aggregateMessages(sqlmin(AM.msg).alias("min_nbr_exch_sms_out"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggOutMin, ["id"])
        aggOutMax = g.aggregateMessages(sqlmax(AM.msg).alias("max_nbr_exch_sms_out"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggOutMax, ["id"])
        aggOutAvg = g.aggregateMessages(avg(AM.msg).alias("avg_nbr_exch_sms_out"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggOutAvg, ["id"])
        aggOutStd = g.aggregateMessages(stddev(AM.msg).alias("std_nbr_exch_sms_out"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggOutStd, ["id"])
        aggOutSum = g.aggregateMessages(sqlsum(AM.msg).alias("sum_nbr_exch_sms_out"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggOutSum, ["id"])

        # In
        aggInCount = g.aggregateMessages(count(AM.msg).alias("nbr_users_exch_sms_in"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggInCount, ["id"])
        aggInMin = g.aggregateMessages(sqlmin(AM.msg).alias("min_nbr_exch_sms_in"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggInMin, ["id"])
        aggInMax = g.aggregateMessages(sqlmax(AM.msg).alias("max_nbr_exch_sms_in"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggInMax, ["id"])
        aggInAvg = g.aggregateMessages(avg(AM.msg).alias("avg_nbr_exch_sms_in"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggInAvg, ["id"])
        aggInStd = g.aggregateMessages(stddev(AM.msg).alias("std_nbr_exch_sms_in"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggInStd, ["id"])
        aggInSum = g.aggregateMessages(sqlsum(AM.msg).alias("sum_nbr_exch_sms_in"), sendToSrc=AM.edge["nbr_exch_sms"])
        data_sms = data_sms.join(aggInSum, ["id"])
        data_sms = data_sms.join(data, ["id"])
        data = data_sms.withColumnRenamed("id", "msisdn")
    elif svc == SVC.VOIX:
        # svc for voix
        data_voix = g.aggregateMessages(count(AM.msg).alias("nbr_users_exch_voix_out"), sendToSrc=AM.edge["total_duration"])
        aggOutMin = g.aggregateMessages(sqlmin(AM.msg).alias("min_total_duration_voix_out"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggOutMin, ["id"])
        aggOutMax = g.aggregateMessages(sqlmax(AM.msg).alias("max_total_duration_voix_out"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggOutMax, ["id"])
        aggOutAvg = g.aggregateMessages(avg(AM.msg).alias("avg_total_duration_voix_out"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggOutAvg, ["id"])
        aggOutStd = g.aggregateMessages(stddev(AM.msg).alias("std_total_duration_voix_out"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggOutStd, ["id"])
        aggOutSum = g.aggregateMessages(sqlsum(AM.msg).alias("sum_total_duration_voix_out"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggOutSum, ["id"])

        # In
        aggInCount = g.aggregateMessages(count(AM.msg).alias("nbr_users_exch_voix_in"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggInCount, ["id"])
        aggInMin = g.aggregateMessages(sqlmin(AM.msg).alias("min_total_duration_voix_in"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggInMin, ["id"])
        aggInMax = g.aggregateMessages(sqlmax(AM.msg).alias("max_total_duration_voix_in"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggInMax, ["id"])
        aggInAvg = g.aggregateMessages(avg(AM.msg).alias("avg_total_duration_voix_in"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggInAvg, ["id"])
        aggInStd = g.aggregateMessages(stddev(AM.msg).alias("std_total_duration_voix_in"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggInStd, ["id"])
        aggInSum = g.aggregateMessages(sqlsum(AM.msg).alias("sum_total_duration_voix_in"), sendToSrc=AM.edge["total_duration"])
        data_voix = data_voix.join(aggInSum, ["id"])
        data_voix = data_voix.join(data, ["id"])
        data = data_voix.withColumnRenamed("id", "msisdn")
    else:
        return    

    data.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password,svc, start_date, end_date, prefix):

    # result file prefix
    for categorie in [Category.LOCAL, Category.INTER, Category.ROAMING]:
        suf = "_" + str(categorie.name).lower()
        svc_suf = "_" + str(svc.name).lower() + "_"
        filename = prefix + svc_suf + start_date + "_to_" + end_date + suf + "_results.csv"
        # create om graph and compute aggregation
        extract_graph_telco_data(spark, td_driver, td_url, td_user, td_password, None, categorie, svc, start_date, end_date, filename)

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
    beg_date = "20220307"
    end_date = "20220313"
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

    for svc in [SVC.DATA]:
        # check if it for new clients
        dates = generate_dates_from_weeks(59, args.edate)
        prefix = "oba_telco_graph_indicators_" + args.edate
        for pair_of_dates in dates:
            sdate = pair_of_dates[0]
            edate = pair_of_dates[1]
            extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, svc, sdate, edate, prefix)

    spark.stop()


if __name__=="__main__":
    main()

