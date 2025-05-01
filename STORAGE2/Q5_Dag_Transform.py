import os
import re
import shutil
import Q5_1_Dag_Import as q5
 
#*******************************************************************************************************
temp_dir="C:/Users/gizemb/Desktop/deneme2"
start_content, start_dags_file = q5.dag_import(temp_dir, "Start_dags")
end_content, end_dags_file = q5.dag_import(temp_dir, "End_dags")
#*******************************************************************************************************
 
def create_dag_file_if_not_exists(dag_name, data_type, dag_file, template_directory):
    try:
        if not os.path.exists(dag_file):
            dag_result, dag_path = q5.dag_import(template_directory, dag_name, cleanup=False)
 
            template_path = os.path.join(template_directory, "dags", f"Format_Type{data_type}.py")
            if not os.path.exists(template_path):
                raise FileNotFoundError(f"Format_Type{data_type}.py şablonu bulunamadı: {template_path}")
 
            shutil.copy(template_path, dag_file)
            print(f"🆕 Yeni DAG oluşturuldu: {dag_file}")
        else:
            dag_path = dag_file
            print(f"📂 DAG dosyası zaten mevcut: {dag_file}")
 
        return not os.path.exists(dag_file)
 
    except Exception as e:
        print(f"[HATA] DAG dosyası oluşturulamadı: {e}")
        import traceback
        traceback.print_exc()
        return False
 
#*******************************************************************************************************
 
def build_operator_block(target_table_name, command_line, dag_name, data_type):
    if data_type == "3":
        return f"""
{target_table_name} = SSHOperator(
    task_id='{target_table_name}',
    ssh_conn_id='ssh_connection',
    command=f'{command_line} {{dayId}}',
    dag=dag_{dag_name},
)
"""
    else:
        return f"""
{target_table_name} = SSHOperator(
    task_id='{target_table_name}',
    ssh_conn_id='ssh_connection',
    command=f'{command_line}',
    dag=dag_{dag_name},
)
"""
 
#*******************************************************************************************************
 
def update_dag_code(dag_code, dag_name, target_table_name, command_line, is_parallel, dag_new, data_type):
    dag_code = dag_code.replace("{{dag_name}}", dag_name)
    dag_code = dag_code.replace("{{table_name}}", target_table_name)
    dag_code = dag_code.replace("{{command}}", command_line)
 
    if dag_new:
        return dag_code
 
    if f"task_id='{target_table_name}'" not in dag_code:
        operator_block = build_operator_block(target_table_name, command_line, dag_name, data_type)
 
        ssh_operator_pattern = r"SSHOperator\s*\(.*?\)\s*"
        matches = list(re.finditer(ssh_operator_pattern, dag_code, re.DOTALL))
 
        if not matches:
            raise ValueError("DAG dosyasında SSHOperator bloğu bulunamadı.")
 
        end_index = matches[-1].end()
        dag_code = dag_code[:end_index] + "\n" + operator_block + dag_code[end_index:]
    else:
        print(f"⚡ {target_table_name} için zaten bir SSHOperator var, yeni eklenmedi.")
 
    if is_parallel.lower() == 'e':
        # Paralel ise listeye ekle
        dag_flow_pattern = re.compile(r">>\s*\[(.*?)\]\s*>>\s*end", re.DOTALL)
 
        def update_parallel_flow(match):
            existing_tasks = [task.strip() for task in match.group(1).split(",") if task.strip()]
            if target_table_name not in existing_tasks:
                existing_tasks.append(target_table_name)
            return f">> [\n    {',\n    '.join(existing_tasks)}\n] >> end"
 
        dag_code = dag_flow_pattern.sub(update_parallel_flow, dag_code)
 
    else:
        # Paralel değilse: en sona task ekle
        # Adım 1: önce listeyi bul
        sequential_pattern = r"(\]\s*>>\s*end)"
 
        # Adım 2: önceki patternin sonuna yeni task ekle
        if re.search(sequential_pattern, dag_code, re.DOTALL):
            dag_code = re.sub(
                sequential_pattern,
                f"] >> {target_table_name} >> end",
                dag_code,
                flags=re.DOTALL
            )
        else:
            raise ValueError("DAG akış yapısı beklenildiği gibi değil!")
 
    return dag_code
 
#*******************************************************************************************************
 
def update_start_dags(dag_name):
    try:
        with open(start_dags_file, "r", encoding="utf-8") as file:
            start_code = file.read()
 
        if f"trigger_{dag_name}" not in start_code:
            trigger_block = f"""
trigger_{dag_name} = TriggerDagRunOperator(
    task_id='trigger_{dag_name}',
    trigger_dag_id='{dag_name}',
    execution_date="{{{{execution_date}}}}",
    wait_for_completion=False,
)
"""
 
            trigger_insertion_point = re.search(r"(start\s*>>\s*sending_email\s*>>\s*\[)", start_code)
            if trigger_insertion_point:
                insert_index = trigger_insertion_point.end()
                start_code = start_code[:insert_index] + f"\n    trigger_{dag_name}," + start_code[insert_index:]
 
            start_code = trigger_block + "\n" + start_code
 
            with open(start_dags_file, "w", encoding="utf-8") as file:
                file.write(start_code)
 
            print(f"✅ start_dags.py güncellendi: {dag_name}")
    except Exception as e:
        print(f"[HATA] start_dags.py güncellerken hata: {e}")
        import traceback
        traceback.print_exc()
 
def update_end_dags(dag_name):
    try:
        with open(end_dags_file, "r", encoding="utf-8") as file:
            end_code = file.read()
 
        if f"end_{dag_name}" not in end_code:
            external_block = f"""
end_{dag_name} = ExternalTaskSensor(
    task_id='end_{dag_name}',
    external_dag_id='{dag_name}',
    external_task_id='end',
    allowed_states=['success'],
    mode='poke',
    timeout=600,
    poke_interval=60,
    execution_date_fn=lambda execution_date: execution_date,
)
"""
            task_list_pattern = r"\[(.*?)\] >> trigger_Datamarts_Type3"
            match = re.search(task_list_pattern, end_code, re.DOTALL)
 
            if match:
                task_list = match.group(1)
                task_list_items = [item.strip() for item in task_list.split(",") if item.strip()]
                if f"end_{dag_name}" not in task_list_items:
                    task_list_items.append(f"end_{dag_name}")
 
                new_task_list = "[\n    " + ",\n    ".join(task_list_items) + "\n] >> trigger_Datamarts_Type3"
                end_code = re.sub(task_list_pattern, new_task_list, end_code, flags=re.DOTALL)
 
            end_code = external_block + "\n" + end_code
 
            with open(end_dags_file, "w", encoding="utf-8") as file:
                file.write(end_code)
 
            print(f"✅ end_dags.py güncellendi: {dag_name}")
    except Exception as e:
        print(f"[HATA] end_dags.py güncellerken hata: {e}")
        import traceback
        traceback.print_exc()
 
#*******************************************************************************************************
 
def create_or_update_dag(dag_name, target_table_name, data_type, is_parallel, dag_directory, template_directory):
    try:
        dag_file = os.path.join(dag_directory, "dags", f"{dag_name}.py")
        dag_new = create_dag_file_if_not_exists(dag_name, data_type, dag_file, template_directory)
 
        # Mevcut veya yeni dosyayı oku
        with open(dag_file, "r", encoding="utf-8") as file:
            dag_code = file.read()
 
        short_dag_name = dag_name.split("_")[0]
        command_line = f"export PYTHONPATH=~/EDW && python3 /home/{{username}}/python_sh/EDW/{short_dag_name}/{target_table_name}.py"
 
        dag_code = update_dag_code(dag_code, dag_name, target_table_name, command_line, is_parallel, dag_new, data_type)
 
        # Güncellenmiş kodu yaz
        with open(dag_file, "w", encoding="utf-8") as file:
            file.write(dag_code)
 
        # Start_dags ve End_dags dosyalarını da güncelle
        update_start_dags(dag_name)
        update_end_dags(dag_name)
 
        print(f"🎉 {target_table_name} tablosu {dag_name} DAG'ına başarıyla entegre edildi.")
 
        # 💬 ŞİMDİ: Her durumda dosyayı yeniden oku ve yazdır
        with open(dag_file, "r", encoding="utf-8") as file:
            final_dag_code = file.read()
 
        print("\n🖨️ Final DAG dosyası içeriği:\n")
        print(final_dag_code)
        print("\n✅ DAG dosyası içeriği terminale başarıyla yazdırıldı.\n")
 
        return final_dag_code
 
    except Exception as e:
        print(f"[Genel Hata] DAG oluşturma/güncelleme hatası: {e}")
        import traceback
        traceback.print_exc()
        return None