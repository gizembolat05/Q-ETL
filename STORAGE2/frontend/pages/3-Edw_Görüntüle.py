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

# Session state değişkenlerini başlat
if 'schema_approved' not in st.session_state:
    st.session_state['schema_approved'] = False

st.title("EDW")

# "edw_script" niteliğini kontrol et ve başlat
if 'edw_script' not in st.session_state:
    st.session_state.edw_script = "Varsayılan değer"  # veya uygun bir başlangıç değeri

# Her sayfa yenilemesinde display_edw_script'i edw_script ile güncelle
# Bu, parametrelerden gelen değerlerin sağ tarafta da görüntülenmesini sağlar
st.session_state.display_edw_script = st.session_state.edw_script

# İki sütun oluştur
edw_cols = st.columns(2)  # İki sütun oluştur

# Sol sütunda düzenleme alanı ve güncelleme butonu
with edw_cols[0]:
    st.subheader("EDW Kodunu Düzenleyin")

    # Mevcut metin uzunluğuna göre minimum yükseklik hesapla
    min_height_edw = max(300, len(st.session_state.edw_script.split('\n')) * 20)

    # Düzenleme alanı
    edw_script_input = st.text_area(
        "", 
        value=st.session_state.edw_script, 
        height=min_height_edw,
        key="edw_input"
    )

    # Güncelleme butonu
    if st.button("EDW Kodunu Güncelle", use_container_width=True, key="edw_update_btn"):
        st.session_state.edw_script = edw_script_input
        st.session_state.display_edw_script = edw_script_input
        st.success("EDW kodu başarıyla güncellendi!")

# Sağ sütunda görüntüleme alanı
with edw_cols[1]:
    st.subheader("EDW Kodu")
    # Burada display_edw_script değişkenini kullanıyoruz
    st.markdown(f"```sql\n{st.session_state.display_edw_script}\n```")

# Schema sayfasında eklenecek kod:
st.write("---")
schema_approval = st.checkbox("EDW kodunu onaylıyorum", value=st.session_state.schema_approved)

# Session state'i güncelle
if schema_approval != st.session_state.schema_approved:
    st.session_state.schema_approved = schema_approval
    if schema_approval:
        st.success("EDW onaylandı!")
    else:
        st.warning("EDW onayı kaldırıldı!")

