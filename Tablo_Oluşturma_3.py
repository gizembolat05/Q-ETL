import pyodbc

def get_column_types(cursor, table_name):
    """Gerçek veri tiplerini almak için INFORMATION_SCHEMA kullan, yetkin yoksa boş döndür."""
    try:
        print(table_name)
        cursor.execute(f"""
            
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}'
        """)
        column_types = {}
        for row in cursor.fetchall():
            col_name, data_type, char_max_len = row
            if data_type in ["char", "varchar", "nvarchar", "nchar"]:
                data_type = f"{data_type}({char_max_len if char_max_len else 'MAX'})"
            column_types[col_name] = data_type.upper()
        
        # Debug için sütun bilgilerini yazdıralım
        print("DB'den Gelen Sütun Tipleri:", column_types)
        return column_types
    except Exception as e:
        print(f"Kolon türleri alınırken hata oluştu (Yetki Yok?): {e}")
        return {}  # Yetkin yoksa boş döndür

def map_sql_type(pyodbc_type):
    """PyODBC type_code -> SQL Server veri tipi eşleştirme."""
    type_mapping = {
        pyodbc.SQL_CHAR: "CHAR(255)",
        pyodbc.SQL_VARCHAR: "VARCHAR(255)",
        pyodbc.SQL_WCHAR: "NVARCHAR(255)",
        pyodbc.SQL_WVARCHAR: "NVARCHAR(255)",
        pyodbc.SQL_SMALLINT: "SMALLINT",
        pyodbc.SQL_INTEGER: "INT",
        pyodbc.SQL_BIGINT: "BIGINT",
        pyodbc.SQL_FLOAT: "FLOAT",
        pyodbc.SQL_REAL: "REAL",
        pyodbc.SQL_DOUBLE: "FLOAT",
        pyodbc.SQL_NUMERIC: "DECIMAL(18,2)",
        pyodbc.SQL_DECIMAL: "DECIMAL(18,2)",
        pyodbc.SQL_TYPE_DATE: "DATE",
        pyodbc.SQL_TYPE_TIMESTAMP: "DATETIME",
        pyodbc.SQL_BIT: "BIT"
    }
    return type_mapping.get(pyodbc_type, "VARCHAR(255)")

def create_table_from_sql(sql_query, db_type, server, database,source_table_name, table_name, timeout=60):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},1433;DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=Yes'
    
    conn = pyodbc.connect(conn_str, timeout=timeout)
    
    try:
        cursor = conn.cursor()

        # Timeout ayarı
        cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000};")

        # SQL sorgusunu çalıştır ve sütun bilgilerini al
        cursor.execute(sql_query)
        columns = [(column[0], column[1]) for column in cursor.description]  # (Sütun adı, PyODBC TypeCode)

        # Gerçek veri tiplerini SQL Server'dan al
        column_types = get_column_types(cursor, source_table_name)

        # CREATE TABLE sorgusunu oluştur
        create_query = f"CREATE TABLE dbo.{table_name} (\n"
        for column_name, pyodbc_type in columns:
            column_type = column_types.get(column_name)  # DB'den bulmaya çalış

            if not column_type:  # Eğer DB'den veri tipi bulunamazsa pyodbc üzerinden map et
                column_type = map_sql_type(pyodbc_type)

            create_query += f"    {column_name} {column_type},\n"
        
        create_query = create_query.rstrip(',\n') + "\n);"

        # Partitioning ekleyelim
        if db_type == 3:  # DayId'ye göre partition
            create_query = create_query.replace(");", ",[ETLDate] [datetime2](7) NOT NULL\n )   [ps_dayId]([DayId]);")
        elif db_type == 2:  # CurrentFlag'e göre partition
            create_query = create_query.replace(");", ",[ETLDate] [datetime2](7) NOT NULL\n )   [ps_currentFlag]([CurrentFlag]);")

        # Temporary tablo oluşturulması
        temp_create_query = create_query.replace(f"CREATE TABLE dbo.{table_name}", f"CREATE TABLE {table_name}_temp")

        # Sonuçları döndür
        print("CREATE TABLE Komutu:")
        print(create_query)
        print("\nTemporary Table Komutu:")
        print(temp_create_query)
        
        return create_query, temp_create_query

    except Exception as e:
        print(f"Bir hata oluştu: {e}")
        return None, None
    finally:
        conn.close()
