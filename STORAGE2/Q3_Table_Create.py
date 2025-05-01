import pyodbc

def get_column_types(cursor, table_name):
    """Gerçek veri tiplerini almak için INFORMATION_SCHEMA kullan, yetkin yoksa boş döndür."""
    try:
        # Köşeli parantezleri kaldır
        table_name = table_name.strip('[]')  # Köşeli parantezleri kaldır

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
        #print("DB'den Gelen Sütun Tipleri:", column_types)
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

def create_table_from_sql(sql_query, data_type, source_server, source_db_name, source_table_name, target_table_name, target_db_name, timeout=60):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={source_server},1433;DATABASE={source_db_name};Trusted_Connection=yes;TrustServerCertificate=Yes'

    conn = pyodbc.connect(conn_str, timeout=timeout)

    try:
        cursor = conn.cursor()
        cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000};")
        cursor.execute(sql_query)
        columns = [(column[0], column[1]) for column in cursor.description]

        column_types = get_column_types(cursor, source_table_name)

        base_query = f"CREATE TABLE dbo.{target_table_name} (\n"
        for column_name, pyodbc_type in columns:
            column_type = column_types.get(column_name)
            if not column_type:
                column_type = map_sql_type(pyodbc_type)
            base_query += f"    {column_name} {column_type},\n"

        if data_type == 2:
            # Görseldeki sabit kolonları ekleyelim
            base_query += (
                "    CurrentFlag BIT,\n"
                "    StartDate DATETIME2(7),\n"
                "    EndDate DATETIME2(7),\n"
                "    ETLDate DATETIME2(7)\n"
            )
            # Sorguyu henüz sonlandırmayalım
            create_query = base_query + ");"

            # Partition ekleme
            create_query = create_query.replace(");", "\n )   [ps_currentFlag]([CurrentFlag]);")

        elif data_type == 3:
            base_query = base_query.rstrip(',\n') + "\n);"
            create_query = base_query
            create_query = create_query.replace(");", "\n )   [ps_dayId]([DayId]);")
        else:
            base_query = base_query.rstrip(',\n') + "\n);"
            create_query = base_query

        temp_create_query = create_query.replace(
            f"CREATE TABLE dbo.{target_table_name}",
            f"CREATE TABLE dbo.{target_table_name}_temp"
        )
        return create_query, temp_create_query

    except Exception as e:
        print(f"Bir hata oluştu: {e}")
        return None, None
    finally:
        conn.close()

def get_select_query_from_create_table(sql_query, data_type, source_server, source_db_name, source_table_name, target_table_name, target_db_name, timeout=60):
    create_query, _ = create_table_from_sql(sql_query, data_type, source_server, source_db_name, 
                                         source_table_name, target_table_name, target_db_name, timeout)

    if not create_query:
        return None

    # CREATE TABLE sorgusundan kolon isimlerini çıkartalım
    lines = create_query.split('\n')
    column_lines = [line.strip() for line in lines if line.strip() and 
                   not line.strip().startswith("CREATE TABLE") and 
                   not line.strip() == ");"]

    # Son satır parantez içeriyorsa düzeltelim
    if column_lines and ");" in column_lines[-1]:
        column_lines[-1] = column_lines[-1].replace(");", "")

    # Kolon isimlerini ayıklayalım
    columns = []
    for line in column_lines:
        if "," in line:
            line = line.rstrip(",")
        parts = line.strip().split(" ", 1)  # Sadece ilk boşluktan böl
        if parts and parts[0]:
            column_name = parts[0].strip()
            columns.append(column_name)

    # SELECT sorgusunu oluşturalım - köşeli parantezli ve alt alta
    select_query = "SELECT "
    for i, column in enumerate(columns):
        if i == 0:
            select_query += f"[{column}]"
        else:
            select_query += f"\n      ,[{column}]"

    select_query += f"\nFROM {target_db_name}.dbo.{target_table_name}"

    return select_query