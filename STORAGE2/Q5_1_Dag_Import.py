import os
import subprocess
import shutil

def dag_import(target_dir, dag_name, cleanup=True):

    repo_url = "https://azuredevops.misyon.com/AgileCollection/DWH/_git/DWH"
    relative_path = f"dags/{dag_name}.py"
    full_file_path = os.path.join(target_dir, relative_path)

    try:
        os.makedirs(target_dir, exist_ok=True)

        subprocess.run(["git", "init"], check=True, cwd=target_dir)
        #subprocess.run(["git", "remote", "remove", "origin"], check=False, cwd=target_dir)
        # dag_import fonksiyonunda, git remote add öncesi:
        result = subprocess.run(["git", "remote"], cwd=target_dir, stdout=subprocess.PIPE, text=True)

        if "origin" not in result.stdout:
            subprocess.run(["git", "remote", "add", "origin", repo_url], check=True, cwd=target_dir)
        else:
            subprocess.run(["git", "config", "core.sparseCheckout", "true"], check=True, cwd=target_dir)

        # Sparse checkout ayarları
        
        #subprocess.run(["git", "config", "core.sparseCheckout", "true"], check=True, cwd=target_dir)

        sparse_file_path = os.path.join(target_dir, ".git", "info", "sparse-checkout")
        with open(sparse_file_path, "w") as f:
            f.write(relative_path.replace("\\", "/") + "\n")

        subprocess.run(["git", "pull", "origin", "master"], check=True, cwd=target_dir)

        # Dosya içeriğini oku
        with open(full_file_path, "r", encoding="utf-8") as file:
            content = file.read()

        # Cleanup işlemleri (isteğe bağlı)
        #if cleanup:
         #   shutil.rmtree(target_dir, ignore_errors=True)

    except subprocess.CalledProcessError as e:
        print("⚠️ Git işleminde hata oluştu:", e)
        content = 0
    except FileNotFoundError:
        print("⚠️ Dosya bulunamadı. Dosya yolunu kontrol edin.")
        content = 0
    except Exception as e:
        print("⚠️ Genel hata:", e)
        content = 0

    return content, full_file_path
