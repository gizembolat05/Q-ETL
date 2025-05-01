# -*- coding: utf-8 -*-
"""
Created on Sun Apr 13 00:24:49 2025
 
@author: tugcei
"""
 
import os
import sys
import Q3_Table_Create  as tbl
import Q2_SQL_Analysis  as sql
import Q4_T1_EDW_Transform as T1
import Q4_T2_EDW_Transform as T2
import Q4_T3_EDW_Transform as T3
import Q4_T4_EDW_Transform as T4
import Q5_Dag_Transform as dag
 
def main(data_type,source_server,source_db_name,sql_query,script_name,target_db_name,subject_area,target_table_name,id_columns,table_description,dag_name,is_parallel):
 
    # Diğer parametreleri al
    sql_query=sql_query
    data_type = data_type
    source_server = source_server
    source_db_name = source_db_name
    script_name = script_name
    target_db_name = target_db_name
    subject_area = subject_area
    target_table_name = target_table_name
    table_description = table_description
 
    # Opsiyonel parametreler
    id_columns = []
 
 
    if data_type == 2 or data_type == 4:
        id_columns_str = input("ID sütunları (virgülle ayrılmış): ")
        id_columns = [col.strip() for col in id_columns_str.split(",")]
 
 
    # Kaynak tabloyu belirleme
    main_table, join_tables = sql.extract_tables_from_sql(sql_query)
    source_table_name = main_table.split('.')[-1] if main_table else 'KaynakTablo'
 
    # Tablo oluşturma scripti
    create_sql, temp_sql = tbl.create_table_from_sql(
        sql_query,
        data_type,
        source_server,
        source_db_name,
        source_table_name,
        target_table_name,
        target_db_name
    )
 
    # SELECT sorgusunu oluştur
    select_query = tbl.get_select_query_from_create_table(
        sql_query,
        data_type,
        source_server,
        source_db_name,
        source_table_name,
        target_table_name,
        target_db_name
    )
 
    # EDW Script üretimi
    if data_type == 1:
        edw_script = T1.generate_tip1_edw_code(
            script_name=script_name,
            sql_query=sql_query,
            source_db_name=source_db_name,
            target_db_name=target_db_name,
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            subject_area=subject_area,
            table_description=table_description
        )
    elif data_type == 2:
        edw_script = T2.generate_tip2_edw_code(
            script_name=script_name,
            sql_query=sql_query,
            source_db_name=source_db_name,
            target_db_name=target_db_name,
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            subject_area=subject_area,
            type2_query=select_query,  
            id_columns=id_columns,
            table_description=table_description
        )
    elif data_type == 3:
        edw_script = T3.generate_tip3_edw_code(
            script_name=script_name,
            sql_query=sql_query,
            source_db_name=source_db_name,
            target_db_name=target_db_name,
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            subject_area=subject_area,
            table_description=table_description
        )
    elif data_type == 4:
        edw_script = T4.generate_tip4_edw_code(
            script_name=script_name,
            sql_query=sql_query,
            source_db_name=source_db_name,
            target_db_name=target_db_name,
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            subject_area=subject_area,
            type4_query=select_query,  
            id_columns=id_columns,
            table_description=table_description
        )
    else:
        edw_script = "-- Desteklenmeyen data_type"
 
 
    # Çıktıları göster
    print("CREATE TABLE:\n", create_sql)
    print("\n TEMP TABLE:\n", temp_sql)
    print(f"\n EDW Tip-{data_type} Script:\n", edw_script)
   
    # DAG OLUŞTURMA
    dag_name = dag_name
    dag_type = str(data_type)
    dag_directory = r"C:\Users\gizemb\Desktop\deneme2"
    template_directory = r"C:\Users\gizemb\Desktop\deneme2"
    #dag_file = dag_file = os.path.join(dag_directory, "dags", f"{dag_name}.py")
    is_parallel = is_parallel  # Default paralel
   
    dag_code=dag.create_or_update_dag(
        dag_name=dag_name,
        target_table_name=target_table_name,
        data_type=str(data_type),
        is_parallel=is_parallel,
        dag_directory=dag_directory,
        template_directory=template_directory
    )
    #print(dag)
    
    return edw_script,create_sql,temp_sql,dag_code
 
#def __main__():
 #   main(data_type,source_server,source_db_name,sql_query,script_name,target_db_name,subject_area,target_table_name,id_columns,table_description,dag_name,is_parallel)
  #  print (main)