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
from dates_generator import generate_previous_week, generate_list_months


def extract_seg_telco_data(spark, oracle_driver, oracle_url, oracle_user, oracle_pwd, month_id, output_path):
    oracle_process = ExtractorSQL(spark, True, oracle_driver, oracle_url, oracle_user, oracle_pwd)
    seg_telco_sql = "select case when length(msisdn)=11 then '22507'||substr(msisdn,4,8) when length(msisdn)=13 then msisdn\
        end msisdn,\
        case when conso_cp_tot+conso_forfait_tot=0 then 'ZERO_VALUE'\
             when conso_cp_tot+conso_forfait_tot<=1000 then 'VERY_LOW_VALUE - ]0-1000]'\
             when conso_cp_tot+conso_forfait_tot<=2500 then 'LOW_VALUE - ]1000-2500]'\
             when conso_cp_tot+conso_forfait_tot<=5000 then 'MIDDLE_VALUE - ]2500-5000]'\
             when conso_cp_tot+conso_forfait_tot<=10000 then 'MIDDLE_HIGH_VALUE - ]5000-10000]'\
             when conso_cp_tot+conso_forfait_tot<=50000 then 'HIGH_VALUE - ]10000-50000]'\
             when conso_cp_tot+conso_forfait_tot>50000 then 'VERY_HIGH_VALUE - ]50000++['\
        end segment_valeur,\
        case when UPPER(PROFIL_ENDPERIOD) like '%COMMUNITY%' or  UPPER(PROFIL_ENDPERIOD) like 'SP_D_BR_NOVA' then 'HYBRIDE-COMMUNITY'\
             when (UPPER(PROFIL_ENDPERIOD) LIKE 'SP_D_OI%' OR UPPER(PROFIL_ENDPERIOD) LIKE '%PERSONNEL%' OR\
                  UPPER(PROFIL_ENDPERIOD) LIKE '%TEST%' OR UPPER(PROFIL_ENDPERIOD) LIKE '%2007' OR \
                  UPPER(PROFIL_ENDPERIOD) LIKE '%INTERNE%') THEN 'HYBRIDE-PERSONNEL' \
            when upper(TYPE_CLIENT_ENDPERIOD) like '%HYBRID%' then 'HYBRIDE-AUTRE'\
            when upper(TYPE_CLIENT_ENDPERIOD) like '%POSTP%' then 'POSTPAID'\
            when UPPER(PROFIL_ENDPERIOD) in ('DAUPHIN','COLIBRI','TIGRE','AIGLE') then 'ANIMALS-'||UPPER(PROFIL_ENDPERIOD)\
            when UPPER(PROFIL_ENDPERIOD) in ('KIT_ELITE','KIT_CLASSIC','TOGO-TOGO','KIT_JEUNE') then 'NOVAMIX-'||UPPER(PROFIL_ENDPERIOD)\
            when UPPER(PROFIL_ENDPERIOD) in ('MAXY','PROXY','LIBERTY','BARA','MAGALI') then 'AGM-'||UPPER(PROFIL_ENDPERIOD)\
            when UPPER(PROFIL_ENDPERIOD) like '%NEO%SECOND%' then 'AGM-'||'NEO_SECONDE'\
            when UPPER(PROFIL_ENDPERIOD) like '%ORANG%INA%' then 'AGM-'||'FUN_INTENS'\
            else 'PREPAID-AUTRES'\
        end  plan_tarifaire,\
        vol_tot_data_ko,\
        conso_cp_tot+conso_forfait_tot arpu_telco,\
        NB_TOT_PASS_DATA nb_pass_data,\
        conso_cp_tot_pass_data+conso_forfait_tot_pass_data arpu_data,\
        NB_TOT_PASS_IX nb_pass_mix,\
        conso_cp_tot_pass_ix+conso_forfait_tot_pass_ix arpu_mix,\
        case when om_rc_amount+rc_montant_tot=0 then 'ZERO'\
             when (conso_cp_tot_pass_via_om+conso_forf_tot_pass_via_om)\
                  /(om_rc_amount+rc_montant_tot)<=0.1 then ']0 - 10%]'\
             when (conso_cp_tot_pass_via_om+conso_forf_tot_pass_via_om)\
                  /(om_rc_amount+rc_montant_tot)<=0.3 then ']10% - 30%]'\
             when (conso_cp_tot_pass_via_om+conso_forf_tot_pass_via_om)\
                  /(om_rc_amount+rc_montant_tot)<=0.5 then ']30% - 50%]'\
             when (conso_cp_tot_pass_via_om+conso_forf_tot_pass_via_om)\
                  /(om_rc_amount+rc_montant_tot)>0.5 then ']50% & ++['\
        end taux_usage_om\
 from csp_oci.vbm_datamart_mly@old_to_csp \
 where month_id='" + month_id + "' and\
       case when length(msisdn)=11 then '22507'||substr(msisdn, 4, 8)\
             when length(msisdn)=13 then msisdn\
        end is not null"
    
    oracle_process.load(seg_telco_sql)
    oracle_process.to_table("oracle_seg_telco")

    seg_telco_sql_term = "select case when length(msisdn)=11 then '22507'||substr(msisdn, 4, 8)\
             when length(msisdn)=13 then msisdn\
        end msisdn_terminal,\
        case when nvl(type_terminal,0) = 4 or VOL_4G_OTA_MO > 0 then '4G'\
             when nvl(type_terminal,0) in (1, 2) then '2G'\
             when nvl(type_terminal,0) = 3 then '3G'\
             when nvl(type_terminal,0) = 0 then 'INCONNU'\
         end type_terminal\
  from RCM_TDB_DATAMART_V3 \
  where month_id='" + month_id + "' and\
        case when length(msisdn)=11 then '22507'||substr(msisdn,4,8)\
             when length(msisdn)=13 then msisdn\
        end is not null"
    
    oracle_process.load(seg_telco_sql_term)
    oracle_process.to_table("oracle_seg_telco_terminals")

    # Joining the terminals with segmentation telco
    oracle_before_df = spark.sql("select msisdn, segment_valeur, plan_tarifaire, vol_tot_data_ko/1024 trafic_data_mo, arpu_telco, nb_pass_data, arpu_data, nb_pass_mix, arpu_mix, taux_usage_om, '" + month_id + "' month_id, type_terminal from oracle_seg_telco join oracle_seg_telco_terminals on oracle_seg_telco.msisdn = oracle_seg_telco_terminals.msisdn_terminal")
    oracle_before_df.createOrReplaceTempView("oracle_seg_telco")

    # Normalizing the data
    oracle_df_oba_norm = spark.sql("""select concat("22507", right(msisdn,8)) as msisdn, segment_valeur, plan_tarifaire, trafic_data_mo, arpu_telco, nb_pass_data, arpu_data, nb_pass_mix, arpu_mix, taux_usage_om, type_terminal, month_id from oracle_seg_telco""")
    oracle_df_oba_norm.createOrReplaceTempView("oracle_seg_telco_norm")

    # Filtering out oba clients data
    oracle_df = spark.sql("""select msisdn, segment_valeur, plan_tarifaire, trafic_data_mo, arpu_telco, nb_pass_data, arpu_data, nb_pass_mix, arpu_mix, taux_usage_om, type_terminal, month_id from oracle_seg_telco_norm inner join oba_clients on oracle_seg_telco_norm.msisdn = oba_clients.Phone""")
    #oracle_df.show()

    oracle_df.toPandas().to_csv(output_path, sep=",", index=False)


def extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, month, prefix):

    # result file prefix
    filename = prefix + month + ".csv"
    extract_seg_telco_data(spark, oracle_driver, oracle_url, oracle_user, oracle_password, month, filename)

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
        months = generate_list_months(6, args.edate)
        prefix = "OBA_DATA_TELCO_NEW_CLIENTS_OF_" + args.edate + "_"
        for month in months:
            extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, month, prefix)
    else:
        sdate = args.sdate
        edate = args.edate

        if args.auto == "true":
            sdate = beg_date
            edate = end_date

        month_id = str(edate)[:2]
        prefix = "OBA_DATA_TELCO_"
        extract_and_copyfile(args, spark, oracle_driver, oracle_url, oracle_user, oracle_password, month_id, prefix)

    # result file prefix
    sdate = args.sdate
    edate = args.edate

    if args.auto == "true":
        sdate = beg_date
        edate = end_date

    month_id = str(edate)[:-2]
    filename = "OBA_DATA_TELCO_" + month_id
    if args.clients.find("new") != -1:
        filename = filename + "_NEW_CLIENTS.csv"
    if args.clients.find("old") != -1:
        filename = filename + ".csv"

    extract_seg_telco_data(spark, oracle_driver, oracle_url, oracle_user, oracle_password, month_id, filename)

    # copying the data to server
    remote_path = args.dest + filename

    # SSH connection
    fast_ssh = FastSshCopy(args.host, args.port, args.user, args.password)
    fast_ssh.connect()
    fast_ssh.send(filename, remote_path)

    print("file: "+ filename + " has been copied to oba.")
    remove(filename)
    print("file: "+ filename + " has been removed.")

    spark.stop()


if __name__=="__main__":
    main()



