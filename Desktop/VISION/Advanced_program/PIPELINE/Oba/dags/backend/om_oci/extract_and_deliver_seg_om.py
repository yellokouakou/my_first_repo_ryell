import argparse
from os.path import join
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, stddev, expr
import numpy as np
from os import remove
import os
import joblib
from sklearn.preprocessing import scale

import sys
sys.path.append(join(Path(__file__).resolve().parents[1], "utils"))

from extractor import ExtractorSQL
from ssh_copy import SshCopy, FastSshCopy
from dates_generator import generate_previous_week, generate_months_range


def segmentation_om(spark, output_path):
    # to be converted in full spark
    data_spark = spark.sql("select * from om_segmentation_data")
    # data_spark.show()
    data = data_spark.toPandas()
    # data.to_csv("donnees_brutes.csv", sep=",", index=False)

    # Récupération de certaines variables devant servir de description des classes
    data1 = data[['MSISDN','DEPARTEMENT_TRAVAIL','REGION_RESIDENCE','ANCIENNETE_MOIS', 'NB_DIST_TRANSACT','ARPU','FREQ_MOIS']]

    # Remplacer toutes les valeurs manquantes par zero
    data.fillna(0, inplace=True)

    # Création de variables a partir des transactions du client
    data['ARPU_OM'] = data.ARPU
    data['MNT_TRANSACTIONS'] = data.MNT_IRTIN+data.MNT_IRTOUT+data.MNT_PAIEMENTFACTURE+data.MNT_B2WIN+data.MNT_B2WOUT+data.MNT_CASHIN+data.MNT_CASHOUT+data.MNT_P2PIN+data.MNT_P2POUT+data.MNT_TOPUP
    data['MNT_TRANSACTIONS_MY'] = data['MNT_TRANSACTIONS']/6
    data['MNT_TRANSACTIONS_MAX'] = data.MAX_IRTIN+data.MAX_IRTOUT+data.MAX_B2WIN+data.MAX_B2WOUT+data.MAX_CASHIN+data.MAX_CASHOUT+data.MAX_P2PIN+data.MAX_P2POUT
    data['MNT_TRANSACTIONS_MIN'] = data.MIN_P2PIN+data.MIN_P2POUT+data.MIN_IRTIN+data.MIN_IRTOUT+data.MIN_CASHIN+data.MIN_CASHOUT+data.MIN_B2WIN+data.MIN_B2WOUT

    # Création des variables concernant les usages des clients
    data['USAGE_TOP_UP'] = data.apply(lambda u: 1 if u["NB_TOPUP"]!=0 else 0,axis=1)
    data['SERVICE_CASH'] = data.apply(lambda u: 1 if u["USAGE_CASHIN"]==1 or u["USAGE_CASHOUT"]==1 else 0,axis=1)
    data['SERVICE_IRT'] = data.apply(lambda u: 1 if u["USAGE_IRTIN"]==1 or u["USAGE_IRTOUT"]==1 else 0,axis=1)
    data['SERVICE_P2P'] = data.apply(lambda u: 1 if u["USAGE_P2P_IN"]==1 or u["USAGE_P2P_OUT"]==1 else 0,axis=1)
    data['SERVICE_VOC'] = data.apply(lambda u: 1 if u["USAGE_B2WOUT"]==1 or u["USAGE_B2WIN"]==1 else 0,axis=1)
    data["SERVICE_ABON"] = data.USAGE_PAIEMENTFACTURE
    data["SERVICE_FRAIS"] = data.USAGE_FRAIS_SCO
    data["SERVICE_TOP_UP"] = data.USAGE_TOP_UP
    data['service'] = data[['SERVICE_CASH','SERVICE_IRT','SERVICE_P2P','SERVICE_VOC','SERVICE_ABON','SERVICE_FRAIS','SERVICE_TOP_UP']].astype('bool').sum(axis=1)

    vectKM=data[['MNT_IRTIN','NB_IRTIN','MNT_IRTOUT','NB_IRTOUT', 'MNT_PAIEMENTFACTURE','NB_PAIEMENTFACTURE','MNT_B2WIN','MNT_B2WOUT',\
               'NB_B2WOUT', 'MNT_P2POUT', 'NB_P2POUT', 'MNT_CASHIN', 'NB_CASHIN', 'MNT_P2PIN', 'NB_P2PIN', 'MNT_CASHOUT', 'NB_CASHOUT',\
               'MNT_TOPUP', 'NB_TOPUP', 'MNT_TRANSACT_IN', 'NB_TRANSACT_IN', 'MNT_TRANSACT_OUT', 'NB_TRANSACT_OUT']]

    # Chargement du modèle entraîné Le modèle entrainé est au format sav
    # on le charge donc pour la prédiction de nouvelles données
    strlen=40
    strformat = "{:<" + str(strlen) + "}"
    print(strformat.format("Chargement des modèles"), end="  ========>  ")
    filename = "tmp_data/models/model_ok.sav"
    loaded_km = joblib.load(filename)
    print("OK")

    # Modele de segmentation
    print(strformat.format("Segmentation"), end="  ========>  ")

    # Standardiser les données
    data_km = vectKM

    # Faire la prédiction avec les nouvelles données
    groupes = loaded_km.predict(data_km)
    model_data = data.copy(deep=True)
    model_data['label'] = groupes

    # Conténation de la base d'origine avec les informations supplémentaires des clients pour la description
    base_sony = pd.concat([model_data,data1],axis=1)

    # Labélisation des segments obtenus
    segment1=base_sony[base_sony['label']==0]
    segment2=base_sony[base_sony['label']==1]
    segment3=base_sony[base_sony['label']==2]
    segment4=base_sony[base_sony['label']==3]
    segment5=base_sony[base_sony['label']==4]
    segment6=base_sony[base_sony['label']==5]
    segment7=base_sony[base_sony['label']==6]

    # Description des segments à partir des services énumérés
    segment1['CATEGORIE']=np.where(segment1['ARPU_OM']<=segment1['ARPU_OM'].mean(),'S11','S41')
    segment5['CATEGORIE']=np.where(segment5['ARPU_OM']<=segment5['ARPU_OM'].mean(),'S21','S32')
    segment6['CATEGORIE']=np.where(segment6['ARPU_OM']>=segment6['ARPU_OM'].mean(),'S12','S22')
    segment4['CATEGORIE']=np.where(segment4['ARPU_OM']<=segment4['ARPU_OM'].mean(),'S42','S51')
    segment2['CATEGORIE']=np.where(segment2['ARPU_OM']>=segment2['ARPU_OM'].mean(),'S31','S61')
    segment7['CATEGORIE']=np.where(segment7['ARPU_OM']<=segment7['ARPU_OM'].mean(),'S52','S72')
    S11=segment1[segment1["CATEGORIE"]=='S11']
    S12=segment6[segment6["CATEGORIE"]=='S12']
    S21=segment5[segment5["CATEGORIE"]=='S21']
    S22=segment6[segment6["CATEGORIE"]=='S22']
    S31=segment2[segment2["CATEGORIE"]=='S31']
    S32=segment5[segment5["CATEGORIE"]=='S32']
    S41=segment1[segment1["CATEGORIE"]=='S41']
    S42=segment4[segment4["CATEGORIE"]=='S42']
    S51=segment4[segment4["CATEGORIE"]=='S51']
    S52=segment7[segment7["CATEGORIE"]=='S52']
    S61=segment2[segment2["CATEGORIE"]=='S61']
    segment3['CATEGORIE']='S71'
    S71=segment3
    S72=segment7[segment7["CATEGORIE"]=='S72']
    CLUSTER1=pd.concat([S11,S12],axis=0)
    CLUSTER1['CLUSTER']='1'
    CLUSTER2=pd.concat([S21,S22],axis=0)
    CLUSTER2['CLUSTER']='2'
    CLUSTER3=pd.concat([S31,S32],axis=0)
    CLUSTER3['CLUSTER']='3'
    CLUSTER4=pd.concat([S41,S42],axis=0)
    CLUSTER4['CLUSTER']='4'
    CLUSTER5=pd.concat([S51,S52],axis=0)
    CLUSTER5['CLUSTER']='5'
    CLUSTER6=S61
    CLUSTER6['CLUSTER']='6'
    CLUSTER7=pd.concat([S71,S72],axis=0)
    CLUSTER7['CLUSTER']='7'

    final_data=pd.concat([CLUSTER1,CLUSTER2,CLUSTER3,CLUSTER4,CLUSTER5,CLUSTER6,CLUSTER7],axis=0)
    final_data.to_csv(output_path, sep=",", index=False)


def extract_sony_om_aggregation_data(spark, oracle_driver, oracle_url, oracle_user, oracle_pwd, beg_date, end_date):
    oracle_process = ExtractorSQL(spark, True, oracle_driver, oracle_url, oracle_user, oracle_pwd)
    om_segmentation_data_sql = "SELECT * FROM DWHDEV.SONY_TELCO_OM_V2_OBA WHERE PERIODE >= '"+ beg_date +"' AND PERIODE <= '"+ end_date +"'"
    oracle_process.load(om_segmentation_data_sql)
    oracle_process.to_table("om_segmentation_data")
    # spark.sql("select * from om_segmentation_data").show()

    # Normalizing the data
    cols = "MNT_IRTIN, NB_IRTIN, MNT_IRTOUT, NB_IRTOUT, MNT_PAIEMENTFACTURE, NB_PAIEMENTFACTURE,\
MNT_B2WIN, MNT_B2WOUT, NB_B2WOUT, NB_B2WIN, MNT_P2POUT, NB_P2POUT, MNT_CASHIN, NB_CASHIN, MNT_P2PIN, NB_P2PIN,\
MNT_CASHOUT, NB_CASHOUT, MNT_TOPUP, NB_TOPUP, MNT_TRANSACT_IN, NB_TRANSACT_IN, MNT_TRANSACT_OUT, NB_TRANSACT_OUT,\
MTN_MY_IRT_IN, MTN_MY_IRT_OUT, MTN_MY_CASHIN, MTN_MY_CASHOUT, MTN_MY_B2W_IN, MTN_MY_B2W_OUT, MTN_MY_TOP_UP, RECENCE_OM_IN,\
RECENCE_OM_OUT, RECENCE_OM_TOTAL, USAGE_B2WIN, USAGE_B2WOUT, USAGE_P2P_IN, USAGE_P2P_OUT, USAGE_TOP_UP, USAGE_CASHIN,\
USAGE_CASHOUT, USAGE_TRANSACT_IN, USAGE_TRANSACT_OUT, USAGE_PAIEMENTFACTURE, USAGE_IRTIN, USAGE_IRTOUT, NB_TRANSACT,\
MAX_P2PIN, MAX_P2POUT, MAX_IRTIN, MAX_IRTOUT, MAX_CASHIN, MAX_CASHOUT, MAX_B2WIN, MAX_B2WOUT, MIN_P2PIN, MIN_P2POUT,\
MIN_IRTIN, MIN_IRTOUT, MIN_CASHIN, MIN_CASHOUT, MIN_B2WIN, MIN_B2WOUT, MNT_FRAIS_SCO, NB_FRAIS_SCO, USAGE_FRAIS_SCO,\
SEXE, AGE_CLIENT, CONSO_CP, CONSO_VOIX, EVD_AMOUNT, OM_AMOUNT, RC_AMOUNT, NB_TOT, NB_DATA, CONSO_DATA, ZONE_RESIDENCE,\
DEPARTEMENT_RESIDENCE, DEPARTEMENT_TRAVAIL, REGION_RESIDENCE, ANCIENNETE_MOIS, RECENCE_JOUR, AUPU, NB_DIST_TRANSACT, ARPU, FREQ_MOIS"
    # norm_sql_spark = "select PERIODE, concat('22507', substr(trim(MSISDN),-1, 8)) as MSISDN, " + cols + " from om_segmentation_data"
    # oracle_norm = spark.sql(norm_sql_spark)
    # oracle_norm.createOrReplaceTempView("om_segmentation_data")

    # Filtering out oba clients data
    filtered_sql = "select MSISDN, PERIODE, " + cols + " from om_segmentation_data inner join oba_clients on om_segmentation_data.msisdn = oba_clients.Phone"
    oracle_df_filtered = spark.sql(filtered_sql)
    # oracle_df_filtered.show()
    oracle_df_filtered.createOrReplaceTempView("om_segmentation_data")


def extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, start, end, prefix):

    # result file prefix
    filename = prefix + start + "_TO_" + end + ".csv"
    
    extract_sony_om_aggregation_data(spark, oracle_driver, oracle_url, oracle_user, oracle_password, start, end)
    segmentation_om(spark, filename)

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
    path_to_oba_clients = "AllCustomers.csv"
    ssh.get(path_to_oba_clients, args.clients)
    clients_data = pd.read_csv(path_to_oba_clients, sep=";")
    clients = spark.createDataFrame(clients_data)
    clients.createOrReplaceTempView("oba_clients")

    # check if it for new clients
    if str(args.clients).find("new") != -1: # contains new clients
        dates_range = generate_months_range(6, args.edate)
        for date_range in dates_range:
            start = date_range[0]
            end = date_range[1]
            prefix = "OBA_SONY_DATA_NEW_CLIENTS_OF_"
            extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, start, end, prefix)
    else:
        sdate = args.sdate
        edate = args.edate

        if args.auto == "true":
            sdate = beg_date
            edate = end_date
       
        date_range = generate_months_range(1,edate) 
        start = date_range[0][0]
        end = date_range[0][1]
        prefix = "OBA_SONY_DATA_"
        extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, start, end, prefix)

    spark.stop()


if __name__=="__main__":
    main()



