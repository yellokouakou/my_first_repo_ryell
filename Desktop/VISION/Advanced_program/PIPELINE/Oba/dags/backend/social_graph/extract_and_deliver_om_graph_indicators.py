import argparse
from create_om_graph import OmGraph
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


def extract_graph_om_data(spark, td_driver, td_url, td_user, td_password, transaction_tag, beg_date, end_date, output_path):
    Graph = OmGraph(spark, False, td_driver, td_url, td_user, td_password, beg_date, end_date, "data_om", "msisdn")
    g = Graph.build_om_graph(transaction_tag)

    agg = g.aggregateMessages(sqlsum(AM.msg).alias("sumInAmount"), sendToDst=AM.edge["total_transaction"])
    aggMaxIn = g.aggregateMessages(sqlmax(AM.msg).alias("maxInAmount"), sendToDst=AM.edge["total_transaction"])
    agg = agg.join(aggMaxIn, ["id"])
    aggMinIn = g.aggregateMessages(sqlmin(AM.msg).alias("minInAmount"), sendToDst=AM.edge["total_transaction"])
    agg = agg.join(aggMinIn, ["id"])
    aggAvgIn = g.aggregateMessages(avg(AM.msg).alias("avgInAmount"), sendToDst=AM.edge["total_transaction"])
    agg = agg.join(aggAvgIn, ["id"])
    aggCountIn = g.aggregateMessages(count(AM.msg).alias("countIn"), sendToDst=AM.edge["total_transaction"])
    agg = agg.join(aggCountIn, ["id"])
    aggSumOut = g.aggregateMessages(sqlsum(AM.msg).alias("sumOutAmount"), sendToSrc=AM.edge["total_transaction"])
    agg = agg.join(aggSumOut, ["id"])
    aggMaxOut = g.aggregateMessages(sqlmax(AM.msg).alias("maxOutAmount"), sendToSrc=AM.edge["total_transaction"])
    agg = agg.join(aggMaxOut, ["id"])
    aggMinOut = g.aggregateMessages(sqlmin(AM.msg).alias("minOutAmount"), sendToSrc=AM.edge["total_transaction"])
    agg = agg.join(aggMinOut, ["id"])
    aggAvgOut = g.aggregateMessages(avg(AM.msg).alias("avgOutAmount"), sendToSrc=AM.edge["total_transaction"])
    agg = agg.join(aggAvgOut, ["id"])
    aggCountOut = g.aggregateMessages(count(AM.msg).alias("countOut"), sendToSrc=AM.edge["total_transaction"])
    agg = agg.join(aggCountOut, ["id"])
    in_degrees = g.inDegrees
    agg = agg.join(in_degrees, ["id"])
    out_degrees = g.outDegrees
    agg = agg.join(out_degrees, ["id"])
    triangle = g.triangleCount().select("id", "count")
    agg = agg.join(triangle, ["id"])
    agg = agg.distinct()
    agg.createOrReplaceTempView("indicators_om_graph")
   
    # Normalizing the data
    agg_norm = spark.sql("""select concat("22507", right(id, 8)) as msisdn, sumInAmount, maxInAmount, minInAmount, avgInAmount, countIn, sumOutAmount, maxOutAmount, minOutAmount, avgOutAmount, countOut, inDegree, outDegree, count as nbTriangles from indicators_om_graph""")
    agg_norm.createOrReplaceTempView("indicators_om_graph_norm")

    # Filtering out oba clients data
    agg_filtered = spark.sql("""select msisdn, sumInAmount, maxInAmount, minInAmount, avgInAmount, countIn, sumOutAmount, maxOutAmount, minOutAmount, avgOutAmount, countOut, inDegree, outDegree, nbTriangles from indicators_om_graph_norm inner join oba_clients on indicators_om_graph_norm.msisdn = oba_clients.Phone""")
    # td_df_filtered.show()
    agg_filtered.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, start_date, end_date, prefix):

    # result file prefix
    filename = prefix + start_date + "_to_" + end_date + "_results.csv"
    
    # create om graph and compute aggregation
    extract_graph_om_data(spark, td_driver, td_url, td_user, td_password, None, start_date, end_date, filename)

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
    destination_folder = "/home/davidtia/incoming/omci/graph_om/"
    beg_date, end_date = generate_previous_week()
    # beg_date = "20220124"
    # end_date = "20220130"
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
        prefix = "oba_om_graph_indicators_newclients_of_" + args.edate + "_"
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

        prefix = "oba_om_graph_indicators_"
        extract_and_copyfile(args, spark, td_driver, td_url, td_user, td_password, sdate, edate, prefix)
    
    spark.stop()


if __name__=="__main__":
    main()
