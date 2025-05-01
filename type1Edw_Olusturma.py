from datetime import datetime
import re

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

def generate_tip1_edw_code(script_name, sql_query, source_db_name, target_db_name, source_table_name, target_table_name, schema="dbo", subject_area="MsyDWH"):
    # Tablo adından script adını oluştur
    script_name = f"{target_table_name}.py"

    # Temp tablo adlarını oluştur
    temp_table_name = f"{target_table_name}_temp"
    new_temp_table_name = f"{target_table_name}_temp_temp"

    # SQL sorgusunda WITH (NOLOCK) olup olmadığını kontrol et ve yoksa ekle
    sql_query = ensure_nolock_in_query(sql_query)

    # Şimdi Tip1 EDW kodunu oluşturalım
    current_date = datetime.now().strftime("%d.%m.%Y")

    edw_code = f"""#***************************** {target_table_name} **********************************

# Type       : Type 1
# Create Date: {current_date}

#************************* 1- KÜTÜPHANELER ***********************************
import Utils.ff_utils as ff
import os
from datetime import datetime
import gc

#************************* 2- PARAMETRE TANIMLARI *****************************
subject_area = "{subject_area}"
script_name = "{script_name}"
schema = "{schema}"

ff_server_properties = ff.read_txt(os.path.join(os.path.expanduser("~"),'EDW','Utils','ff_server_properties'))
source_username, source_password, source_server_ip, bank_name = ff_server_properties[0], ff_server_properties[1], ff_server_properties[2], ff_server_properties[7]
target_username, target_password, target_server_ip, target_db_name = ff_server_properties[3], ff_server_properties[4], ff_server_properties[5], ff_server_properties[6]

source_db_name = f"{source_db_name}-{{bank_name}}"
target_db_name = f"{{target_db_name}}"

source_table_name = '{source_table_name}'
target_table_name = '{target_table_name}'
temp_table_name = '{temp_table_name}'
new_temp_table_name = '{new_temp_table_name}'

source_engine = ff.create_url(source_server_ip, source_db_name, source_username, source_password)
target_engine = ff.create_url(target_server_ip, target_db_name, target_username, target_password)

etl_log = ff.create_etl_log()
records_log = ff.create_records_log()

#************************* 3- SQL TANIMLARI *****************************
source_sql_stmt = f'''{sql_query}'''

#************************* 4- PROSES ÇALIŞMALARI ve LOGLAMA **********************
ff.write_etl_log(subject_area, script_name, etl_log, "SQL okuma", "Başladı")

source_df = ff.extract_table_to_df(source_sql_stmt, source_engine)

ff.write_etl_log(subject_area, script_name, etl_log, "SQL okuma", "Bitti")

target_df = source_df

ff.write_records_log(subject_area, script_name, records_log, source_df, source_db_name, schema, source_table_name, target_df, target_db_name, schema, target_table_name)

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloyu truncate etme", "Başladı")

ff.truncate_table(schema, temp_table_name, target_engine)

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloyu truncate etme", "Bitti")

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloya yazma", "Başladı")

target_df['ETLDate'] = datetime.now()

ff.load_df_into_dwh(target_df, schema, temp_table_name, target_engine, "append")

ff.write_etl_log(subject_area, script_name, etl_log, "Temp tabloya yazma", "Bitti")

ff.write_etl_log(subject_area, script_name, etl_log, "Tabloları rename etme", "Başladı")

ff.rename_table(schema, target_table_name, new_temp_table_name, target_engine)

ff.rename_table(schema, temp_table_name, target_table_name, target_engine)

ff.rename_table(schema, new_temp_table_name, temp_table_name, target_engine)

ff.write_etl_log(subject_area, script_name, etl_log, "Tabloları rename etme", "Bitti")

ff.load_df_into_dwh(records_log, schema, 'dw_RecordsCountLog', target_engine, "append")

ff.load_df_into_dwh(etl_log, schema, 'dw_ETLLog', target_engine, "append")


#************************* 5- DATA FRAME TEMİZLİKLERİ **********************
del source_df
del target_df
del records_log
del etl_log

gc.collect()
"""

    return edw_code