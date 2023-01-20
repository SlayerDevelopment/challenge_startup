import glob
from datetime import datetime

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

input_folder = "/home/bryan/mysql_inputs"
file_name = "postcodesgeo.csv"


def load_and_clean_csv():
    dataframe = pd.read_csv(input_folder + "/" + file_name, sep=",")
    dataframe_idx = dataframe[(dataframe["lat"] == 0) & (dataframe["lon"] == 0)].index
    dataframe.drop(dataframe_idx, inplace=True)
    dataframe.drop_duplicates(inplace=True)
    dataframe_obj = dataframe.select_dtypes(["object"])
    dataframe[dataframe_obj.columns] = dataframe_obj.apply(lambda x: x.str.strip())
    dataframe.rename(columns={"lat": "latitude", "lon": "longitude"}, inplace=True)
    number_of_files = 21
    size = 100000
    for i in range(number_of_files):
        df = dataframe[size * i : size * (i + 1)]
        df.to_csv(f"/home/bryan/inputs/postcodesgeo_clean_{i+1}.csv", index=False)
    print("ARCHIVOS CREADOS PARA " + str(len(dataframe)) + " REGISTROS")


with DAG(
    "DAG_CHALLENGER_CLEAN",
    start_date=datetime(2023, 1, 16),
    catchup=False,
    schedule="00 22 * * *",
    default_args={"retries": 2, "mysql_conn_id": "bia_challenger_db"},
) as challenger_clean_dag:
    start_task = EmptyOperator(task_id="CHALLENGER_CLEAN_START_TASK")
    end_task = EmptyOperator(task_id="CHALLENGER_CLEAN_END_TASK")
    clean_csv_task = PythonOperator(
        task_id="CHALLENGER_CLEAN_TASK",
        python_callable=load_and_clean_csv,
    )
    trigger_load_dag = TriggerDagRunOperator(
        task_id="CHALLENGER_TRIGGER_TASK",
        trigger_dag_id="DAG_CHALLENGER_LOAD",
        wait_for_completion=False,
    )
    start_task >> clean_csv_task >> trigger_load_dag >> end_task


def counts_csv():
    dir_path = "/home/bryan/inputs/postcodesgeo_clean_*.csv"
    paths_lst = glob.glob(dir_path)
    return paths_lst


def return_api_url(longitude, latitude):
    return "https://api.postcodes.io/postcodes?lon=%s&lat=%s" % (longitude, latitude)


def insert_codes_func(table_key, code, description, mysql_cursor, mysql_conn):
    if code != None:
        mysql_cursor.execute(
            "SELECT COUNT(1) FROM bia_challenge."
            + table_key
            + " WHERE code='"
            + code
            + "'"
        )
        count_coordinates = mysql_cursor.fetchone()
    else:
        return 0
    if count_coordinates[0] == 1:
        mysql_cursor.execute(
            "SELECT id FROM bia_challenge." + table_key + " WHERE code='" + code + "'"
        )
        coordinates_id = mysql_cursor.fetchone()[0]
    else:
        if description == None and code == None:
            print(
                "ERROR NO SE PUEDE INSERTAR DOS VALORES NULOS EN LA TABLA: " + table_key
            )
            return 0
        elif description == None and code != None:
            mysql_cursor.execute(
                "INSERT INTO bia_challenge."
                + table_key
                + "(code,description) VALUES('"
                + code
                + "',NULL)"
            )
            coordinates_id = mysql_cursor.lastrowid
            mysql_conn.commit()
        else:
            mysql_cursor.execute(
                "INSERT INTO bia_challenge."
                + table_key
                + "(code,description) VALUES('"
                + code
                + "','"
                + description.replace("'","''")
                + "')"
            )
            coordinates_id = mysql_cursor.lastrowid
            mysql_conn.commit()
    return coordinates_id


def insert_codes_func_ccg(
    table_key, code, description, code_2, mysql_cursor, mysql_conn
):
    if code != None:
        mysql_cursor.execute(
            "SELECT COUNT(1) FROM bia_challenge."
            + table_key
            + " WHERE code='"
            + code
            + "' AND code_2='"
            + code_2
            + "'"
        )
        count_coordinates = mysql_cursor.fetchone()
    else:
        return 0
    if count_coordinates[0] == 1:
        mysql_cursor.execute(
            "SELECT id FROM bia_challenge."
            + table_key
            + " WHERE code='"
            + code
            + "' AND code_2='"
            + code_2
            + "'"
        )
        coordinates_id = mysql_cursor.fetchone()[0]
    else:
        if description == None and code == None and code_2 == None:
            print(
                "ERROR NO SE PUEDE INSERTAR DOS VALORES NULOS EN LA TABLA: " + table_key
            )
            return 0
        elif description == None and code != None and code_2 != None:
            mysql_cursor.execute(
                "INSERT INTO bia_challenge."
                + table_key
                + "(code,code_2,description) VALUES('"
                + code
                + "','"
                + code_2
                + "',NULL)"
            )
            coordinates_id = mysql_cursor.lastrowid
            mysql_conn.commit()
        else:
            mysql_cursor.execute(
                "INSERT INTO bia_challenge."
                + table_key
                + "(code,code_2,description) VALUES('"
                + code
                + "','"
                + code_2
                + "','"
                + description
                + "')"
            )
            coordinates_id = mysql_cursor.lastrowid
            mysql_conn.commit()
    return coordinates_id


def check_None(value):
    if value == None:
        return "NULL"
    else:
        return value


def check_None_strings(value):
    if value == None:
        return "NULL"
    else:
        return "'" + value + "'"


def save_row(longitude, latitude, results):
    mysql_hook = MySqlHook("bia_challenger_db")
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()
    # COORDINATES
    mysql_cursor.execute(
        "SELECT COUNT(1) FROM bia_challenge.coordinates WHERE longitude='"
        + str(longitude)
        + "' AND latitude='"
        + str(latitude)
        + "'"
    )
    count_coordinates = mysql_cursor.fetchone()
    if count_coordinates[0] == 1:
        mysql_cursor.execute(
            "SELECT id FROM bia_challenge.coordinates WHERE longitude='"
            + str(longitude)
            + "' AND latitude='"
            + str(latitude)
            + "'"
        )
        coordinates_id = mysql_cursor.fetchone()[0]
    else:
        mysql_cursor.execute(
            "INSERT INTO bia_challenge.coordinates(longitude,latitude) VALUES("
            + str(longitude)
            + ","
            + str(latitude)
            + ")"
        )
        mysql_conn.commit()
        coordinates_id = mysql_cursor.lastrowid
    # codes
    codes_dict = {}
    for result in results:
        for key in result["codes"]:
            if key not in ["ccg", "ccg_id", "lau2"]:
                key_id = insert_codes_func(
                    key, result["codes"][key], result[key], mysql_cursor, mysql_conn
                )
                if key_id != 0:
                    codes_dict[key] = key_id
                else:
                    codes_dict[key] = None
                    print(
                        "Error en el registro de la llave: ",
                        key,
                        " Para la coordenada(",
                        longitude,
                        ",",
                        latitude,
                        ")",
                    )
            elif key == "ccg":
                key_id = insert_codes_func_ccg(
                    key,
                    result["codes"][key],
                    result[key],
                    result["codes"]["ccg_id"],
                    mysql_cursor,
                    mysql_conn,
                )
                if key_id != 0:
                    codes_dict[key] = key_id
                else:
                    codes_dict[key] = None
                    print(
                        "Error en el registro de la llave: ",
                        key,
                        " Para la coordenada(",
                        longitude,
                        ",",
                        latitude,
                        ")",
                    )
            elif key != "ccg_id":
                codes_dict[key] = result["codes"][key]
        # POSTCODES
        mysql_cursor.execute(
            "SELECT COUNT(1) FROM bia_challenge.postcode WHERE postcode='"
            + result["postcode"]
            + "' AND coordinate_id="
            + str(coordinates_id)
        )
        count_postcode = mysql_cursor.fetchone()
        if count_postcode[0] == 1:
            print("POSTCODE YA REGISTRADO")
        else:
            mysql_cursor.execute(
                "INSERT INTO bia_challenge.postcode("
                + "postcode,quality,eastings,northings,country,nhs_ha,"
                + "european_electoral_region,primary_care_trust,region,"
                + "incode,outcode,date_of_introduction,lau2,distance,"
                + "lsoa_id,msoa_id,parliamentary_constituency_id,admin_district_id,"
                + "parish_id,admin_county_id,admin_ward_id,ced_id,ccg_id,nuts_id,pfa_id,coordinate_id) VALUES('"
                + result["postcode"]
                + "',"
                + str(check_None(result["quality"]))
                + ","
                + str(check_None(result["eastings"]))
                + ","
                + str(check_None(result["northings"]))
                + ","
                + check_None_strings(result["country"])
                + ","
                + check_None_strings(result["nhs_ha"])
                + ","
                + check_None_strings(result["european_electoral_region"])
                + ","
                + check_None_strings(result["primary_care_trust"])
                + ","
                + check_None_strings(result["region"])
                + ","
                + check_None_strings(result["incode"])
                + ","
                + check_None_strings(result["outcode"])
                + ","
                + check_None_strings(result["date_of_introduction"])
                + ","
                + check_None_strings(codes_dict["lau2"])
                + ","
                + str(check_None(result["distance"]))
                + ","
                + str(check_None(codes_dict["lsoa"]))
                + ","
                + str(check_None(codes_dict["msoa"]))
                + ","
                + str(check_None(codes_dict["parliamentary_constituency"]))
                + ","
                + str(check_None(codes_dict["admin_district"]))
                + ","
                + str(check_None(codes_dict["parish"]))
                + ","
                + str(check_None(codes_dict["admin_county"]))
                + ","
                + str(check_None(codes_dict["admin_ward"]))
                + ","
                + str(check_None(codes_dict["ced"]))
                + ","
                + str(check_None(codes_dict["ccg"]))
                + ","
                + str(check_None(codes_dict["nuts"]))
                + ","
                + str(check_None(codes_dict["pfa"]))
                + ","
                + str(check_None(coordinates_id))
                + ")"
            )
            mysql_conn.commit()

    mysql_cursor.close()
    mysql_conn.commit()
    mysql_conn.close()
    print(
        "POSTCODES OF LATITUDE: "
        + str(latitude)
        + "- LONGITUDE: "
        + str(longitude)
        + " SAVED"
    )


def return_result(longitude, latitude):
    response = requests.get(return_api_url(longitude, latitude))
    if response.json()["result"] != None:
        save_row(longitude, latitude, response.json()["result"])
        return "GUARDADO"
    else:
        return "EQUIVOCADO"


def req_pandas_by_row(path):
    dataframe = pd.read_csv(path, sep=",")
    dataframe = dataframe.iloc[:100]
    dataframe["result"] = dataframe.apply(
        lambda row: return_result(row["longitude"], row["latitude"]), axis=1
    )
    print("REGISTROS GUARDADOS")


with DAG(
    "DAG_CHALLENGER_LOAD",
    start_date=datetime(2023, 1, 16),
    catchup=False,
    schedule=None,
    default_args={"retries": 2, "mysql_conn_id": "bia_challenger_db"},
) as challenger_load_dag:
    start_task = EmptyOperator(task_id="CHALLENGER_LOAD_START_TASK")
    end_task = EmptyOperator(task_id="CHALLENGER_LOAD_END_TASK")
    for path in counts_csv():
        csv = path.replace(".csv", "")
        csv = csv.split("/")
        csv = csv[4]
        get_csv_to_send_req = PythonOperator(
            task_id=f"REQ_FOR_{csv}",
            python_callable=req_pandas_by_row,
            op_kwargs={"path": path},
        )
        start_task >> get_csv_to_send_req >> end_task
