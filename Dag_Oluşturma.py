import os
import re
import shutil


print("Dag Oluşturma")
dag_directory = "C:/Users/gizemb/Desktop/STORAGE-DAG/dags"
template_directory = "C:/Users/gizemb/Desktop/STORAGE-DAG/template"

def get_user_inputs():
    dag_name = input("DAG adını girin: ").strip()
    table_name = input("Eklemek istediğiniz tablo adını girin: ").strip()
    dag_type = input("DAG tipi nedir? (1, 2, 3, 4): ").strip()
    is_parallel = input("Bu tablo diğerleriyle paralel mi çalışacak? (e/h): ").strip()
    return dag_name, table_name, dag_type, is_parallel

def create_dag_file_if_not_exists(dag_name, dag_type, dag_file, template_directory):
    try:
        if os.path.exists(dag_file):
            return False

        template_file = os.path.join(template_directory, f"Type{dag_type}.py")
        if not os.path.exists(template_file):
            raise FileNotFoundError(f"Type{dag_type}.py şablonu bulunamadı.")

        shutil.copy(template_file, dag_file)
        print(f"Yeni DAG oluşturuldu: {dag_file}")
        return True
    except FileNotFoundError as e:
        print(f"[HATA] Dosya bulunamadı: {e}")
        raise
    except PermissionError:
        print(f"[HATA] Dosya kopyalama izni yok: {dag_file}")
        raise
    except Exception as e:
        print(f"[HATA] DAG dosyası oluşturulurken bir hata oluştu: {e}")
        raise

def build_operator_block(table_name, command_line, dag_name):
        return f"""
{table_name} = SSHOperator(
    task_id='{table_name}',
    ssh_conn_id='ssh_connection',
    command=f'{command_line} {{dayId}}',
    dag=dag_{dag_name},
)
"""

def update_dag_code(dag_code, dag_name, table_name, command_line, is_parallel, dag_new):
    dag_code = dag_code.replace("{{dag_name}}", dag_name)
    dag_code = dag_code.replace("{{table_name}}", table_name)
    dag_code = dag_code.replace("{{command}}", command_line)

    operator_block = build_operator_block(table_name, command_line, dag_name)

    ssh_operator_pattern = r"SSHOperator\s*\(.*?\)\s*"
    matches = list(re.finditer(ssh_operator_pattern, dag_code, re.DOTALL))

    if not matches:
        raise ValueError("DAG dosyasında SSHOperator bloğu bulunamadı.")

    end_index = matches[-1].end()
    dag_code = dag_code[:end_index] + "\n" + operator_block + dag_code[end_index:]

    dag_flow_pattern = re.compile(r">>\s*\[(.*?)\]\s*>>\s*end", re.DOTALL)
    if is_parallel == 'e':
        if dag_new:
            dag_code = dag_flow_pattern.sub(f">> [{table_name}] >> end", dag_code)
        else:
            dag_code = dag_flow_pattern.sub(
                lambda m: f">> [{m.group(1).strip()}, {table_name}] >> end", dag_code
            )
    else:
        linear_pattern = re.compile(r"(>>\s*end)")
        dag_code = linear_pattern.sub(f">> {table_name} >> end", dag_code)

    return dag_code

def main():
    dag_directory = "C:/Users/gizemb/Desktop/STORAGE-DAG/dags"
    template_directory = "C:/Users/gizemb/Desktop/STORAGE-DAG/template"

    try:
        print("Dag Oluşturma")
        dag_name, table_name, dag_type, is_parallel = get_user_inputs()
        dag_file = os.path.join(dag_directory, f"{dag_name}.py")
        dag_new = create_dag_file_if_not_exists(dag_name, dag_type, dag_file, template_directory)

        with open(dag_file, "r", encoding="utf-8") as file:
            dag_code = file.read()

        short_dag_name = dag_name.split("_")[0]
        command_line = f"export PYTHONPATH=~/EDW && python3 /home/{{username}}/python_sh/EDW/{short_dag_name}/{table_name}.py"

        dag_code = update_dag_code(dag_code, dag_name, table_name, command_line, is_parallel, dag_new)

        with open(dag_file, "w", encoding="utf-8") as file:
            file.write(dag_code)

        print("\n--- Güncellenmiş DAG Kodu ---\n")
        print(dag_code)
        print(f"{table_name} tablosu {dag_name} DAG'ına başarıyla eklendi.")

    except FileNotFoundError as fnf_error:
        print(f"[HATA] {fnf_error}")
    except Exception as e:
        print(f"[Genel Hata] Bir şeyler ters gitti : {e}")

if __name__ == "__main__":
    main()
