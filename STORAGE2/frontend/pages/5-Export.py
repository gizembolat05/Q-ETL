import streamlit as st

st.set_page_config(layout="wide")
st.title("Export")

# Session state değişkenlerini başlat
if 'dag_approved' not in st.session_state:
    st.session_state['dag_approved'] = False

if 'sql_approved' not in st.session_state:
    st.session_state['sql_approved'] = False

if 'schema_approved' not in st.session_state:
    st.session_state['schema_approved'] = False

# Onay durumlarını kontrol et
all_approved = (
    st.session_state['dag_approved'] and 
    st.session_state['sql_approved'] and 
    st.session_state['schema_approved']
)

# Onay durumlarını göster
st.write("### Onay Durumları")
col1, col2, col3 = st.columns(3)

with col1:
    if st.session_state['dag_approved']:
        st.success("DAG Onaylandı ✓")
    else:
        st.error("DAG Onaylanmadı ✗")

with col2:
    if st.session_state['sql_approved']:
        st.success("SQL Onaylandı ✓")
    else:
        st.error("SQL Onaylanmadı ✗")

with col3:
    if st.session_state['schema_approved']:
        st.success("Schema Onaylandı ✓")
    else:
        st.error("Schema Onaylanmadı ✗")

st.write("---")

# Export butonu
if all_approved:
    if st.button("Export", use_container_width=True):
        # Export işlemleri burada gerçekleştirilecek
        st.success("Tüm değişiklikler başarıyla export edildi!")

        # Burada export işlemlerinizi gerçekleştirebilirsiniz
        # Örneğin:
        # - Dosyalara yazma
        # - Veritabanına gönderme
        # - API çağrıları yapma

        # Export sonrası isteğe bağlı olarak onayları sıfırlama
        # st.session_state['dag_approved'] = False
        # st.session_state['sql_approved'] = False
        # st.session_state['schema_approved'] = False
else:
    st.warning("Export işlemi için tüm alanlar onaylanmalıdır. Lütfen eksik onayları tamamlayın.")

    # Eksik onayları listele
    missing_approvals = []
    if not st.session_state['dag_approved']:
        missing_approvals.append("DAG")
    if not st.session_state['sql_approved']:
        missing_approvals.append("SQL")
    if not st.session_state['schema_approved']:
        missing_approvals.append("Schema")

    st.write(f"Eksik onaylar: {', '.join(missing_approvals)}")