import pandas as pd 
import numpy as np 
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

file1=r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\CPSC_Enrollment_Info_2022_01.csv"
file22=r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\CPSC_Contract_Info_2022_01.csv"
file2=r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\CPSC_Enrollment_Info_2023_01.csv"
file23=r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\CPSC_Contract_Info_2023_01.csv"

def datafr(file):
    df=pd.read_csv(file)
    print(df.memory_usage(index=False, deep=True))
    df = df.replace('*', np.nan)
    df['State'] = df['State'].astype('category')
    df['County'] = df['County'].astype('category')
    df['FIPS State County Code'] = df['FIPS State County Code'].astype('category')
    df['SSA State County Code'].memory_usage(index=False, deep=True)
    df['SSA State County Code']=df['SSA State County Code'].astype('int32')
    df['Plan ID']=df['Plan ID'].astype('int16')
    df['Enrollment']=df['Enrollment'].astype('int16', errors='ignore')
    df['Enrollment']=df['Enrollment'].replace(np.nan,0)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.lower()
    print(df.memory_usage(index=False, deep=True))
    return df

df22=datafr(file1)
df22.to_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Enroll22.csv")  


df23=datafr(file2)
df23.to_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Enroll23.csv")  


def contrfr(file):
    cont1=pd.read_csv(file, encoding="ISO-8859-1")
    print(cont1.memory_usage(index=False, deep=True))
    cont1 = cont1.replace('*', np.nan)
    cat_var=['Contract ID','Contract Number', 'Plan Type','Organization Type','Offers Part D','SNP Plan','EGHP',  'Parent Organization']
    for columns in cat_var:
        try:
            cont1[columns] = cont1[columns].astype('category') 
        except:
            pass
    cont1['Contract Effective Date']=pd.to_datetime(cont1['Contract Effective Date'])
    cont1= cont1.replace('*', np.nan)
    cont1.columns = cont1.columns.str.replace(' ', '_')
    cont1.columns = cont1.columns.str.lower()
    print(cont1.memory_usage(index=False, deep=True))
    print(cont1.columns)
    return cont1

df22_1=contrfr(file22)
df23_1=contrfr(file23)
df22_1.to_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Contr22.csv")  
df23_1.to_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Contr23.csv")  

#create schema to create sql table

df22_1=pd.read_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Contr22.csv")  
df23_1=pd.read_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Contr23.csv")  

df22=pd.read_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Enroll22.csv")  
df23=pd.read_csv(r"C:\Users\manas\Dropbox\HP_Desktop\D Drive\Data\Twitter\Farzad MA growth 2022-2023\Enroll23.csv")  

df22.enrollment.dtype
df22['enrollment'].value_counts()
df22['enrollment']=df22['enrollment'].astype('int16', errors='ignore')
df23['enrollment']=df23['enrollment'].astype('int16', errors='ignore')


# create table without data first
engine=create_engine('postgresql://postgres:password@localhost:5432/farzad')
print(pd.io.sql.get_schema (df22, name="enroll22", con=engine))
print(pd.io.sql.get_schema (df22, name="enroll23", con=engine))
df22.head(n=0).to_sql(name='enroll22', con=engine, if_exists='replace', index=False)
df23.head(n=0).to_sql(name='enroll23', con=engine, if_exists='replace', index=False)
df22_1.head(n=0).to_sql(name='contract22', con=engine, if_exists='replace', index=False)
df23_1.head(n=0).to_sql(name='contract23', con=engine, if_exists='replace', index=False)

df_iter=pd.read_csv('C:\\Users\\manas\\Dropbox\\HP_Desktop\\D Drive\\Data\\Twitter\\Farzad MA growth 2022-2023\\Enroll22.csv', iterator=True, chunksize=100) 
dfat=next(df_iter) 
dfat.to_sql('enroll22', con=conn, if_exists='append')
        print("inserted another chunk.....")

ak22=pd.DataFrame(df22.groupby('state')['enrollment'].sum())
ak23=pd.DataFrame(df23.groupby('state')['enrollment'].sum())

akat=ak22.merge(ak23, left_on='state', right_on='state', suffixes=('_22', '_23'))
akat['delta']=akat['enrollment_23']-akat['enrollment_22']
akat['MA membership change %']=round(akat['delta']*100/akat['enrollment_22'],2)
akat.reset_index(inplace=True)
akat= akat[akat['state']!="AS"]
akat= akat[akat['state']!="MP"]
akat= akat[akat['state']!="GU"]


import plotly.express as px
fig = px.choropleth(akat,
                    locations='state', 
                    locationmode="USA-states", 
                    scope="usa",
                    color='MA membership change %',
                    color_continuous_scale="Viridis_r", 
                    
                    )
fig.show()
fig.update_layout(
      title_text = 'Change in Jan 2023 MA compared to Jan 2022, by State',
      title_font_family="Times New Roman",
      title_font_size = 22,
      title_font_color="black", 
      title_x=0.45, 
         )

fig.update_layout(
        autosize=False,
        margin = dict(
                l=0,
                r=0,
                b=0,
                t=0,
                pad=4,
                autoexpand=True
            ),
            width=800)

###################turns out this stuff was not necessary...done for learning#################

def db_sql(fileref, sql_table):
    conn_str='postgresql://postgres:password@localhost:5432/farzad'
    engine=create_engine('postgresql://postgres:password@localhost:5432/farzad')
    conn = engine.connect()
    dfat=pd.read_csv(fileref) 
    dfat = dfat.replace('*', np.nan)
    dfat.to_sql(sql_table, con=conn, if_exists='replace')
    print("inserted another chunk.....")



#    while True:                   
#        df_iter=pd.read_csv('C:\\Users\\manas\\Dropbox\\HP_Desktop\\D Drive\\Data\\Twitter\\Farzad MA growth 2022-2023\\contr22.csv', iterator=True, chunksize=100000) 
#        dfat=next(df_iter) 
#        dfat.to_sql(sql_table, con=conn, if_exists='append')
#        print("inserted another chunk.....")
#    conn.close()

db_sql(fileref=r'C:\\Users\\manas\\Dropbox\\HP_Desktop\\D Drive\\Data\\Twitter\\Farzad MA growth 2022-2023\\Enroll22.csv',sql_table='enroll22')
db_sql(fileref=r'C:\\Users\\manas\\Dropbox\\HP_Desktop\\D Drive\\Data\\Twitter\\Farzad MA growth 2022-2023\\Enroll23.csv',sql_table='enroll23')
db_sql(fileref=r'C:\\Users\\manas\\Dropbox\\HP_Desktop\\D Drive\\Data\\Twitter\\Farzad MA growth 2022-2023\\Contr22.csv', sql_table='contract22')
db_sql(fileref=r'C:\\Users\\manas\\Dropbox\\HP_Desktop\\D Drive\\Data\\Twitter\\Farzad MA growth 2022-2023\\Contr23.csv', sql_table='contract23')



conn_str='postgresql://postgres:password@localhost:5432/farzad'
engine=create_engine('postgresql://postgres:password@localhost:5432/farzad')
conn = psycopg2.connect(conn_str)
conn.autocommit = True
cursor = conn.cursor()

sql12 = """
select * from enroll23
"""
cursor.execute(sql12)
#conn.close()
colnames = [desc[0] for desc in cursor.description]
print(colnames)


#find the tables in the database   
cursor.execute("""SELECT relname FROM pg_class WHERE relkind='r'
                  AND relname !~ '^(pg_|sql_)';""") # "rel" is short for relation.

tables = [i[0] for i in cursor.fetchall()] # A list() of tables.
print(tables)

#drop columns
sql2='''ALTER TABLE enroll22  DROP COLUMN index;'''
cursor.execute(sql2)

#Find enrolees  columns
sql2='''SELECT state, 
SUM (enrollment)  
FROM enroll22 
GROUP BY state;'''
cursor.execute(sql2)
cursor.fetchall()
