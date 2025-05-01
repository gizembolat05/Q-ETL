import streamlit as st
import sys
import os

# Boşluklu klasörün tam yolunu belirt
modul_yolu = os.path.abspath("C:/Users/gizemb/Desktop/Desktop/DWH_GelistirmeAlani/Geliştirme Alanı/STORAGE/STORAGE2")

# Python'un sistem yoluna ekle
if modul_yolu not in sys.path:
    sys.path.append(modul_yolu)

# Artık import edebilirsin
#from Q1_Main import main as q1
import Q1_Main as q1

st.set_page_config(page_title="Detay", page_icon="📋")
st.title("Seçilen Tip Bilgileri")

# Tip seçimi yapılmamışsa ana sayfaya yönlendir
if "secilen_tip" not in st.session_state:
    st.warning("Lütfen önce bir tip seçiniz.")
    st.stop()

secilen_tip = st.session_state["secilen_tip"]
st.subheader(f"Seçilen Tip: {secilen_tip}")

# Ortak alanları baştan tanımlayalım
data_type = None
source_server = st.text_input("Kaynak sunucu adı:")
source_db_name = st.text_input("Kaynak veritabanı adı:")
sql_query = st.text_area("SQL Query:")
script_name = st.text_input("Script adı:")
target_db_name = st.text_input("Hedef veritabanı adı:")
subject_area = st.text_input("Konu alanı:")
target_table_name = st.text_input("Hedef tablo adı:")
table_description = st.text_area("Tablonun açıklaması:")
dag_name = st.text_input("Dag adı:")
is_parallel = st.radio("Bu tablo diğerleriyle paralel mi çalışacak?",
                       options=["e", "h"],
                       format_func=lambda x: "Evet" if x == "e" else "Hayır",
                       horizontal=True)

id_columns = None  # Bazı tipler için boş kalacak

if secilen_tip == "tip1":
    data_type = 1

elif secilen_tip == "tip2":
    data_type = 2
    id_columns = st.text_input("Id Columns")

elif secilen_tip == "tip3":
    data_type = 3

elif secilen_tip == "tip4":
    data_type = 4
    id_columns = st.text_input("Id Columns")


# GÖNDER butonu ve BOŞ ALAN kontrolü
if st.button("Gönder"):
    # Zorunlu alanları bir listede topla
    required_fields = [
        source_server, source_db_name, sql_query, script_name,
        target_db_name, subject_area, target_table_name,
        table_description, dag_name
    ]
    if secilen_tip in ["tip2", "tip4"]:
        required_fields.append(id_columns)

    # Boş alan kontrolü
    if any(field.strip() == "" for field in required_fields):
        st.warning("Lütfen tüm zorunlu alanları doldurunuz.")
    else:
        # Hepsi doluysa main fonksiyonunu çağır
        
        st.session_state.edw_script,st.session_state.create_sql,st.session_state.temp_sql,st.session_state.dag_code=q1.main(data_type, source_server, source_db_name, sql_query, script_name,
             target_db_name, subject_area, target_table_name, id_columns,
             table_description, dag_name, is_parallel)
        st.success("İşlem başarıyla tamamlandı :)")
 



























































