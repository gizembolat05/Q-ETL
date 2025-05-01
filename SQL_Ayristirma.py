import re


def extract_tables_from_sql(sql_query):
    # SQL sorgusunda "FROM" ve "JOIN" ifadeleriyle belirtilen tablo isimlerini çıkartacağız.
    pattern = re.compile(r'(FROM|JOIN)\s+([a-zA-Z0-9_\.\[\]]+)', re.IGNORECASE)

    # SQL sorgusundaki alt sorguları ayıklamak için
    subquery_pattern = re.compile(r'\((SELECT.*?)\)', re.IGNORECASE | re.DOTALL)
    subqueries = re.findall(subquery_pattern, sql_query)

    # Tabloları toplayacağız
    main_table = None
    join_tables = []

    # Alt sorgulardaki tabloları çıkaralım
    for subquery in subqueries:
        subquery_tables = extract_tables_from_sql(subquery)
        # Alt sorgulardan gelen tabloları join_tables listesine ekleyelim
        join_tables.extend(subquery_tables)

    # Ana sorgudaki tabloları çıkaralım
    main_query_tables = re.findall(pattern, sql_query)
    for table in main_query_tables:
        operation, table_name = table
        # Eğer tablo ismi parantez içindeyse, bu bir alt sorgudur, geçerli tabloyu almak için alt sorgu çözümü yapılır.
        if not table_name.startswith('('):
            if operation.upper() == 'FROM':
                # FROM ifadesiyle gelen ilk tablo ana tablo olarak kabul edilir
                if main_table is None:
                    main_table = table_name
                else:
                    # Eğer FROM ifadesi birden fazla kez varsa, bu bir alt sorgu olabilir
                    join_tables.append(table_name)
            elif operation.upper() == 'JOIN':
                join_tables.append(table_name)

    # Tabloları benzersiz hale getirelim
    join_tables = list(set(join_tables))

    return main_table, join_tables
