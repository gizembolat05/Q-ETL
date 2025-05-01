import streamlit as st
import os
import sys

# Boşluklu klasörün tam yolunu belirt
modul_yolu = os.path.abspath("C:/Users/gizemb/Desktop/Desktop/DWH_GelistirmeAlani/Geliştirme Alanı/STORAGE/STORAGE2")

# Python'un sistem yoluna ekle
if modul_yolu not in sys.path:
    sys.path.append(modul_yolu)

# Artık import edebilirsin
from Q1_Main import main as q1

st.set_page_config(layout="wide")  # Sayfayı geniş modda aç

# Session state değişkenlerini başlat
if 'dag_approved' not in st.session_state:
    st.session_state['dag_approved'] = False

st.title("Dags")

# "dag_code" niteliğini kontrol et ve başlat
if 'dag_code' not in st.session_state:
    st.session_state.dag_code = "Varsayılan değer"  # veya uygun bir başlangıç değeri

# "display_code" niteliğini kontrol et ve başlat (görüntülenen kod için)
if 'display_code' not in st.session_state:
    st.session_state.display_code = st.session_state.dag_code

# İki sütun oluştur - genişlik oranlarını ayarlayabilirsiniz
col1, col2 = st.columns([1, 1])  # Eşit genişlikte iki sütun

# Mevcut metin uzunluğuna göre minimum yükseklik hesapla
min_height = max(300, len(st.session_state.dag_code.split('\n')) * 20)

# Sol sütunda düzenleme alanı
with col1:
    st.subheader("DAG Kodunu Düzenle")
    # height parametresini dinamik olarak ayarla
    dag_code_input = st.text_area("", st.session_state.dag_code, height=min_height)

    # Container içinde buton oluştur - daha iyi yerleşim için
    with st.container():
        # Güncelleme butonu
        if st.button("Güncelle", use_container_width=True):
            st.session_state.dag_code = dag_code_input
            st.session_state.display_code = dag_code_input
            st.success("DAG kodu başarıyla güncellendi!")

# Sağ sütunda görüntüleme alanı
with col2:
    st.subheader("DAG Kodu")
    # Code bloğunu bir container içine al
    with st.container():
        st.markdown(f"```sql\n{st.session_state.display_code}\n```")


# Onay kutusu ekle
st.write("---")
dag_approval = st.checkbox("DAG kodunu onaylıyorum", value=st.session_state.dag_approved)

# Session state'i güncelle
if dag_approval != st.session_state.dag_approved:
    st.session_state.dag_approved = dag_approval
    if dag_approval:
        st.success("DAG kodu onaylandı!")
    else:
        st.warning("DAG kodu onayı kaldırıldı!")
