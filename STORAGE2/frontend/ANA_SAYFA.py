import streamlit as st
import time

# Sayfa ayarı
st.set_page_config(page_title="Tip Seçimi", page_icon="🧭", layout="centered")

st.title("Tip Seçimi")

tip = st.selectbox("Bir tip seçiniz:", ["tip1", "tip2", "tip3", "tip4"])

# Seçilen tipe göre açıklamalar
tip_aciklamalari = {
    "tip1": "Tip 1 : İçinde çok fazla veri tutmayan ve genellikle tanım/uzun süre değişmeyecek verileri tutan tablolardır. (Örn: Ürün listesi,DK Listesi gibi)",
    "tip2": "Tip 2 : Geçmiş veriler ile mevcut verilerin kıyaslanarak işlem yapıldığı tablolardır. Slow Change Dimesion (SCD) bu alanda işlem görür. (Örn: Müşteri tablosu)",
    "tip3": "Tip 3 : Tarihsel veri saklayan tablolardır. İlgili günün görünümü veri tabanına alınır. (Örn: Bakiye raporları)",
    "tip4": "Tip 4 : Geri valörlü işlemlerin tutulduğu tablolardır. Tip 2 ile benzerlik gösterir"
}

# Seçili tip varsa göster
if tip:
    aciklama = tip_aciklamalari.get(tip, "Seçilen tip hakkında bilgi bulunamadı.")
    
    # Ortalanmış kutu
    st.markdown(
        f"""
        <div style='
            display: flex;
            justify-content: center;
            align-items: center;
            height: 200px;
        '>
            <div style='
                background-color: ORANGE;
                padding: 30px;
                border-radius: 15px;
                color: white;
                font-size: 18px;
                text-align: center;
                font-weight: bold;
                width: 400px;
            '>
                {aciklama}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

# Buton ortalamak için container kullanalım
col1, col2, col3 = st.columns([1,2,1])

with col2:
    if st.button("Devam Et", use_container_width=True):
        st.session_state["secilen_tip"] = tip

        # Progress Bar gösterimi
        progress_placeholder = st.empty()

        with st.spinner("Yükleniyor..."):
            for i in range(101):
                progress_html = f"""
                <style>
                    .progress-container {{
                        width: 100%;
                        height: 30px;
                        background-color: #f0f2f6;
                        border-radius: 15px;
                        margin: 10px 0;
                        overflow: hidden;
                    }}
                    .progress-bar {{
                        height: 100%;
                        width: {i}%;
                        background-color: orange;
                        border-radius: 15px;
                    }}
                    .progress-text {{
                        text-align: center;
                        margin-top: 5px;
                        font-weight: bold;
                        color: #333;
                    }}
                </style>
                <div class="progress-container">
                    <div class="progress-bar"></div>
                </div>
                <div class="progress-text">%{i}</div>
                """
                progress_placeholder.markdown(progress_html, unsafe_allow_html=True)
                time.sleep(0.01)

        time.sleep(0.5)
        st.switch_page("pages/1-Parametreler.py")
