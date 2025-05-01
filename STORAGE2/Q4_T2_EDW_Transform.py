#-----------------------------------------------------Tip 2 EDW----------------------------------------------------------------------------
from datetime import datetime
import re
import os


def ensure_nolock_in_query(sql_query):
    """
    SQL sorgusunda WITH (NOLOCK) ifadesinin olup olmadığını kontrol eder ve yoksa ekler.
    
    Hem FROM ifadelerinden sonra hem de JOIN ifadelerinden sonra ekler.
    """
    # FROM ve JOIN ifadeleri için regex desenleri
    from_pattern = re.compile(r'(FROM\s+\[?[\w\d]+\]?\.\[?[\w\d]+\]?\.\[?[\w\d]+\]?|FROM\s+\[?[\w\d]+\]?)', re.IGNORECASE)
    join_pattern = re.compile(r'(JOIN\s+\[?[\w\d]+\]?\.\[?[\w\d]+\]?\.\[?[\w\d]+\]?|JOIN\s+\[?[\w\d]+\]?)', re.IGNORECASE)
    
    # WITH (NOLOCK) ekleme işlemi
    def add_nolock(match):
        table_clause = match.group(1)
        if 'WITH (NOLOCK)' not in table_clause.upper():
            return f"{table_clause} WITH (NOLOCK)"
        return table_clause
    
    # FROM ifadelerine WITH (NOLOCK) ekle
    sql_query = from_pattern.sub(add_nolock, sql_query)
    
    # JOIN ifadelerine WITH (NOLOCK) ekle
    sql_query = join_pattern.sub(add_nolock, sql_query)
    
    return sql_query


def generate_tip2_edw_code(script_name, sql_query,type2_query, source_db_name, target_db_name, source_table_name, target_table_name,table_description, id_columns, data_type=2, schema="dbo", subject_area="MSYDWH"):
    
    tip2_paremeters=["StartDate", "EndDate", "CurrentFlag", "ETLDate"]
    # Tablo adından script adını oluştur
    script_name = f"{target_table_name}.py"

    # Temp tablo adlarını oluştur
    temp_table_name = f"{target_table_name}_temp"
    new_temp_table_name = f"{target_table_name}_temp_temp"

    # SQL sorgusunda WITH (NOLOCK) olup olmadığını kontrol et ve yoksa ekle
    sql_query = ensure_nolock_in_query(sql_query)

    current_date = datetime.now().strftime("%d.%m.%Y")
    data_type=2
    #Kullanıcı adı alınıyor
    kullanici_adi = os.getlogin()

    #Tip 2 EDW kodu

    edw_code = f"""#***************************** {target_table_name} **********************************

#Type : Type 2
#Create Date : {current_date}
#Coder : {kullanici_adi}
#Table Description : {table_description}
#************************* 1- KÜTÜPHANELER *********************************** 
import Utils.ff_utils as ff
import os
import pandas as pd
from pandas.util import hash_pandas_object
import gc

#************************* 2- PARAMETRE TANIMLARI *****************************
subject_area = "{subject_area}" 
script_name = "{script_name}" 
schema = "{schema}"

ff_server_properties = ff.read_txt(os.path.join(os.path.expanduser("~"),'EDW','Utils','ff_server_properties'))
source_username, source_password, source_server_ip, bank_name = ff_server_properties[0], ff_server_properties[1], ff_server_properties[2], ff_server_properties[7]
target_username, target_password, target_server_ip, target_db_name = ff_server_properties[3], ff_server_properties[4], ff_server_properties[5], ff_server_properties[6]

source_db_name = "{source_db_name}" 
target_db_name = f"{{target_db_name}}"

source_table_name = '{source_table_name}' 
target_table_name = '{target_table_name}' 
temp_table_name = '{temp_table_name}' 
new_temp_table_name = '{new_temp_table_name}'
id_columns = {id_columns}

source_engine = ff.create_url(source_server_ip, source_db_name, source_username, source_password)
target_engine = ff.create_url(target_server_ip, target_db_name, target_username, target_password)

etl_log = ff.create_etl_log()
records_log = ff.create_records_log()

#************************* 3- SQL TANIMLARI *****************************
source_sql_stmt = f'''{sql_query}'''

target_sql_stmt = f'''{type2_query}'''

ff.write_etl_log(subject_area, script_name, etl_log, "SQL okuma", "Başladı")

source_df = ff.extract_table_to_df(source_sql_stmt, source_engine)
target_df = ff.extract_table_to_df(target_sql_stmt, target_engine)

ff.write_etl_log(subject_area, script_name, etl_log, "SQL okuma", "Bitti")

ff.write_etl_log(subject_area, script_name, etl_log,  "Transform", "Başladı")

list_of_dicts = []
source_columns = source_df.columns.tolist()
target_columns = target_df.columns.tolist()

zero_flag_df = target_df[target_df["CurrentFlag"] == int(0)]
one_flag_df = target_df[target_df["CurrentFlag"] == int(1)]
dropped_columns = one_flag_df[[*id_columns, "StartDate", "EndDate", "CurrentFlag", "ETLDate"]]

hashed_source_df = source_df.copy()
hashed_source_df = hashed_source_df.sort_index(axis=1)
hashed_target_df = one_flag_df.drop(columns=["StartDate", "EndDate", "CurrentFlag", "ETLDate"])
hashed_target_df = hashed_target_df.sort_index(axis=1)

hashed_source_df["Hash"] = hash_pandas_object(hashed_source_df, index=False)
hashed_target_df["Hash"] = hash_pandas_object(hashed_target_df, index=False)

difference_df, not_updated_df, updated_df, deleted_df, merged_columns = ff.compare_hashed_columns(id_columns, hashed_source_df, hashed_target_df)

source_suffixed_columns, target_suffixed_columns = ff.rename_columns(id_columns,merged_columns,source_columns,target_columns)

difference_dict = ff.type2_process_dataframes(difference_df, "difference_df", source_suffixed_columns, target_columns, target_suffixed_columns)
list_of_dicts.extend(difference_dict)

not_updated_df = pd.merge(not_updated_df,dropped_columns, how='inner', on=id_columns)
not_updated_dict = ff.type2_process_dataframes(not_updated_df, "not_updated_df", source_suffixed_columns, target_columns, target_suffixed_columns)
list_of_dicts.extend(not_updated_dict)

updated_df = pd.merge(updated_df,dropped_columns, how='inner', on=id_columns)
updated_dict = ff.type2_process_dataframes(updated_df, "updated_df", source_suffixed_columns, target_columns, target_suffixed_columns)
list_of_dicts.extend(updated_dict)

deleted_df = pd.merge(deleted_df,dropped_columns, how='inner', on=id_columns)
deleted_dict = ff.type2_process_dataframes(deleted_df, "deleted_df", source_suffixed_columns, target_columns, target_suffixed_columns)
list_of_dicts.extend(deleted_dict)

list_of_dicts.extend(zero_flag_df.to_dict('records'))

if list_of_dicts:
    final_df = pd.DataFrame(list_of_dicts)

    final_df = final_df.drop(columns="Hash")

else:
    final_df = pd.DataFrame(columns=target_columns)

ff.write_etl_log(subject_area, script_name, etl_log, "Transform", "Bitti")

target_df = final_df

target_df_num = target_df[target_df["CurrentFlag"] == int(1)]

ff.write_records_log(subject_area, script_name, records_log, source_df, source_db_name, schema, source_table_name, target_df, target_db_name, schema, target_table_name)

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloyu truncate etme", "Başladı")

ff.truncate_table(schema, temp_table_name, target_engine)

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloyu truncate etme", "Bitti")

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloya yazma", "Başladı")

ff.load_df_into_dwh(target_df, schema, temp_table_name, target_engine, "append")

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloya yazma", "Bitti")

ff.write_etl_log(subject_area, script_name, etl_log, "Tabloları rename etme", "Başladı")

ff.rename_table(schema, target_table_name, new_temp_table_name, target_engine)

ff.rename_table(schema, temp_table_name, target_table_name, target_engine)

ff.rename_table(schema, new_temp_table_name, temp_table_name, target_engine)

ff.write_etl_log(subject_area, script_name, etl_log, "Tabloları rename etme", "Bitti")

ff.load_df_into_dwh(records_log, "dbo", 'dw_RecordsCountLog', target_engine, "append")

ff.load_df_into_dwh(etl_log, "dbo", 'dw_ETLLog', target_engine, "append")

del source_df
del target_df
del zero_flag_df
del one_flag_df
del hashed_source_df
del hashed_target_df
del difference_df
del not_updated_df
del updated_df
del deleted_df
del final_df
del records_log
del etl_log

gc.collect()

gc.collect() """

    return edw_code