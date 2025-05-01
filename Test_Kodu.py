import Tablo_Oluşturma_3 as tb
import SQL_Ayristirma as sq

server='MSY-T-FMPDB'
database='MxTransfers-misyonx'
Create_Table_Name='dw_MisyonOutgoingRPS'
TableType=3

sql_query='''
select * from [dbo].[MisyonOutgoingRPS]
'''
#***********************************************************************************************
Main_Table , Join_Table = sq.extract_tables_from_sql(sql_query)
noneschema_table_name = Main_Table.split('.')[-1].replace('[', '').replace(']', '')  #Şemasız şekilde tablo adı alınıyor

print(noneschema_table_name)
print(Join_Table)


#***********************************************************************************************
tb.create_table_from_sql(sql_query, TableType, server, database, noneschema_table_name,Create_Table_Name)