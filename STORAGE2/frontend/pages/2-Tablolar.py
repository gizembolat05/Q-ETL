# 3-Edw_Görüntüle.py
import streamlit as st
import sys
import os

# Sayfayı geniş modda aç
st.set_page_config(layout="wide")

# Boşluklu klasörün tam yolunu belirt
modul_yolu = os.path.abspath("C:/Users/gizemb/Desktop/Desktop/DWH_GelistirmeAlani/Geliştirme Alanı/STORAGE/STORAGE2")

# Python'un sistem yoluna ekle
if modul_yolu not in sys.path:
    sys.path.append(modul_yolu)

# Artık import edebilirsin
from Q1_Main import main as q1

# En başta session state'i başlat
if 'sql_approved' not in st.session_state:
    st.session_state['sql_approved'] = False

st.title("EDW")

# "create_sql" ve "temp_sql" session state değişkenlerini başlat
if 'create_sql' not in st.session_state:
    st.session_state.create_sql = "CREATE TABLE example (\n  id INT,\n  name VARCHAR(100)\n);"

# Görüntüleme için ayrı değişken - HER ZAMAN create_sql ile aynı değeri alır
# Bu satır her sayfa yenilemesinde çalışacak
st.session_state.display_create_sql = st.session_state.create_sql

if 'temp_sql' not in st.session_state:
    st.session_state.temp_sql = "CREATE TEMP TABLE temp_example AS\nSELECT * FROM example\nWHERE id > 0;"

# Görüntüleme için ayrı değişken - HER ZAMAN temp_sql ile aynı değeri alır
# Bu satır her sayfa yenilemesinde çalışacak
st.session_state.display_temp_sql = st.session_state.temp_sql

# CREATE TABLE için
st.header("CREATE TABLE")
create_cols = st.columns(2)  # İki sütun oluştur

# Sol sütunda düzenleme alanı ve güncelleme butonu
with create_cols[0]:
    # Mevcut metin uzunluğuna göre minimum yükseklik hesapla
    min_height_create = max(200, len(st.session_state.create_sql.split('\n')) * 20)

    # Düzenleme alanı
    create_sql_input = st.text_area(
        "SQL kodunu düzenleyin:", 
        value=st.session_state.create_sql, 
        height=min_height_create,
        key="create_input"
    )

    # Güncelleme butonu
    if st.button("CREATE TABLE Güncelle", use_container_width=True, key="create_update_btn"):
        st.session_state.create_sql = create_sql_input
        st.session_state.display_create_sql = create_sql_input
        st.success("CREATE TABLE kodu başarıyla güncellendi!")

# Sağ sütunda görüntüleme alanı
with create_cols[1]:
    st.subheader("SQL Kodu")
    # Burada display_create_sql değişkenini kullanıyoruz
    st.markdown(f"```sql\n{st.session_state.display_create_sql}\n```")

# Ayırıcı çizgi
st.markdown("---")

# TEMP TABLE için
st.header("TEMP TABLE")
temp_cols = st.columns(2)  # İki sütun oluştur

# Sol sütunda düzenleme alanı ve güncelleme butonu
with temp_cols[0]:
    # Mevcut metin uzunluğuna göre minimum yükseklik hesapla
    min_height_temp = max(200, len(st.session_state.temp_sql.split('\n')) * 20)

    # Düzenleme alanı
    temp_sql_input = st.text_area(
        "SQL kodunu düzenleyin:", 
        value=st.session_state.temp_sql,
        height=min_height_temp,
        key="temp_input"
    )

    # Güncelleme butonu
    if st.button("TEMP TABLE Güncelle", use_container_width=True, key="temp_update_btn"):
        st.session_state.temp_sql = temp_sql_input
        st.session_state.display_temp_sql = temp_sql_input
        st.success("TEMP TABLE kodu başarıyla güncellendi!")

# Sağ sütunda görüntüleme alanı
with temp_cols[1]:
    st.subheader("SQL Kodu")
    # Burada display_temp_sql değişkenini kullanıyoruz
    st.markdown(f"```sql\n{st.session_state.display_temp_sql}\n```")


# Sayfanın sonunda onay kutusu ekle
st.write("---")
sql_approval = st.checkbox("SQL kodunu onaylıyorum", value=st.session_state['sql_approved'])

# Session state'i güncelle
if sql_approval:
    st.session_state['sql_approved'] = True
    st.success("SQL kodu onaylandı!")
else:
    if st.session_state['sql_approved']:  # Eğer daha önce onaylanmışsa ve şimdi onay kaldırıldıysa
        st.session_state['sql_approved'] = False
        st.warning("SQL kodu onayı kaldırıldı!")