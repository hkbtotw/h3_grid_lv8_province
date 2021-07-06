from h3 import h3
from Database_Population import *
from datetime import datetime, date, timedelta
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from csv_join_tambon import Reverse_GeoCoding
from Credential import *
import numpy as np
import os
import ast
import pandas as pd
import pickle
import glob
from sys import exit
import warnings

warnings.filterwarnings('ignore')

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))



def ConvertGeometryCoordinate(n):

    latList=[]
    lonList=[]
    #print(' n  : ',n, ' ---- ',type(n))
    n2=ast.literal_eval(str(n))
    #print(list(n2['coordinates'][0]), ' ----- ',type(list(n2['coordinates'][0])) )#,list(zip(*n.exterior.coords.xy)))

    for m in list(n2['coordinates'][0]):
        coor=list(m)
        #print(' --- ',coor)
        latList.append(coor[0])
        lonList.append(coor[1])

    
    #Change coordinate system from  epsg:32647 is  UTM
    UTM47N=pyproj.CRS("EPSG:32647")
    #Change coordinate system to  . epsg:4326 is lat lon
    wgs84=pyproj.CRS("EPSG:4326")

    xx, yy=pyproj.transform(wgs84,UTM47N,  latList, lonList )
    #print(xx, ' ---- ',yy)
    
    polygon_geom = Polygon(zip(xx,yy))
    #print(' ----  > ',polygon_geom)
    return polygon_geom

def GetFileNameList(file_path):
    fileList=glob.glob(file_path+"*.data")
    #print(' ----- ',len(fileList))
    filenameList=[]
    for n in fileList:
        file_dummy=n.split('\\')
        filenameList.append(file_dummy[ len(file_dummy)-1 ])

    del file_dummy, fileList    
    return filenameList

def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

def AssignPopulationToHex(idIn, dfagg):
    dfDummy=dfagg[dfagg['hex_id']==idIn].copy().reset_index(drop=True)
    dfDummy.set_index('hex_id', inplace=True)
    #print(' ==> ',dfDummy.head(10))
    if(len(dfDummy)>1):
        print(' ==> ',dfDummy)
    
    totalsum=dfDummy.sum()
    #print(' sum : ',totalsum['population'], ' ===  ',type(totalsum['population']))
    del dfDummy
    return totalsum['population']

def Write_H3_Grid_Province(df_input, conn1):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
   
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn1.commit()

    cursor.close()
    #conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Province(df_input,conn2):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province]  """
    cursor=conn2.cursor()
    cursor.execute(sql)
    conn2.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn2.commit()

    cursor.close()

    print('------------Complete WriteDB-------------')

def Write_H3_Grid_Province_PAT(df_input, conn1):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
   
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT](	

     [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
      ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
      ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['prov_idn']
      ,row['amphoe_idn']
      ,row['tambon_idn']
      ,row['DBCreatedAt']
        )
    conn1.commit()

    cursor.close()
    #conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Province_2(df_input,conn2):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2]  """
    cursor=conn2.cursor()
    cursor.execute(sql)
    conn2.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
        ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
              ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn2.commit()

    cursor.close()

    print('------------Complete WriteDB-------------')

def Write_H3_Grid_Lv9_Province_PAT(df_input, conn1):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
   
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Grid_Lv9_Province_PAT]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_Lv9_Province_PAT](	

     [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
      ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
      ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['prov_idn']
      ,row['amphoe_idn']
      ,row['tambon_idn']
      ,row['DBCreatedAt']
        )
    conn1.commit()

    cursor.close()
    #conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Lv9_Province_2(df_input,conn2):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv9_Province_2]  """
    cursor=conn2.cursor()
    cursor.execute(sql)
    conn2.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv9_Province_2](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
        ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
              ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn2.commit()

    cursor.close()

    print('------------Complete WriteDB-------------')

# Read external complementary data to present on each grid
def Get_Facebook_Population(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population(province)    

    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_Youth_15_24(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_Youth_15_24(province)   

    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_elderly_60_plus(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_elderly_60_plus(province)   

    if(len(dfIn)>0):        
        print('There are elder population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No elder population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_children_under_five(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_children_under_five(province)   

    if(len(dfIn)>0):        
        print('There are under five population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No under five population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_men(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_men(province)   

    if(len(dfIn)>0):        
        print('There are men population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No men population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_women(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_women(province)   

    if(len(dfIn)>0):        
        print('There are women population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No women population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex


########################################################################################################
######  Input ----  ####################################################################################
# SQL connection for writing data to database
conn = connect_tad

# level 8 covers approx 1 km2
h3_level=9   

# Select if using the specific provinces
# if_all_provinces=1 => Use all provinces in boundary_data
# if_all_provinces=2 => Incase, previous run not complete, Continue running from what being left off from previous run.
if_all_provinces=2

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\boundary_data\\'
temp_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\temp\\'
write_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\shapefile\\'
qgis_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\qgis_shapefile\\'

#######################################################################################################

###### Start from Scratch or Continue from previous incomplete runs
if(if_all_provinces==1):
    # Read province boundary data
    print(' --- USE ALL PROVINCES IN BOUNDARY DATA --- ')
    filenameList=GetFileNameList(file_path)
    print(' ---> ',filenameList)
    previousCompleteList=[]
elif(if_all_provinces==2):
    print(' --- Continue from Previous incomplete run  --- ')
    allFilenameList=GetFileNameList(file_path)          
    dfContinue=pd.read_csv(temp_path+'continue.csv')
    completeFlg=dfContinue['completeFlg'].head(1).values[0]
    #print(' continue : ',dfContinue, ' --- flag : ',completeFlg)
    if(completeFlg==0):
        previousCompleteList=list(dfContinue['Province'].unique())
        print(' completeList : ',previousCompleteList)
        continueList = np.setdiff1d( allFilenameList, previousCompleteList)   # find elements in allFilenameList not in dfContinue
        print(' continueList : ',continueList)
        pd.DataFrame(continueList, columns=['Province']).to_csv(temp_path+'incomplete.csv') 
        filenameList=continueList
        del allFilenameList, dfContinue
        del completeFlg, continueList
    else:
        print(' **************************************** ')
        print('  ----  Runs   actually  COMPLETE  ----   ')
        print(' **************************************** ')
        exit(0)
else:
    print(' **************************************** ')
    print('  ----  PROBLEM WITH BOUNDARY DATA ----   ')
    print(' **************************************** ')
    exit(0)

completeList=[]+previousCompleteList
for file_name in filenameList:  #[:2]:
    ################# format : file_name='boundary_ชลบุรี.data'

    province=file_name.split('_')[1].split('.')[0]
    print(' - ',province)

    with open(file_path+file_name,'rb') as filehandle:
        testlist=pickle.load(filehandle)

    hexList=[]
    for coor in testlist:  
        geoJson = {"coordinates": [coor], "type": "Polygon"}   
            
        hexagons = list(h3.polyfill(geoJson,h3_level))
        print(' ==> ',len(hexagons))
        hexList.append(hexagons)

    #### Distinct hexagons to find the complete set of hexagons  for MultiPolygon
    totalList=[]
    for n in hexList:
        totalList=totalList+n
        #print(len(n),' ==> ',len(totalList))
    totalList=list(set(totalList))
    print(' ==> ',len(totalList))
    hexagons=totalList

    # Create dataframe with one columns from hexagons (total hex_id of the selected province) , named it dfHex
    dfHex=pd.DataFrame(hexagons, columns=['hex_id'])
    #dfHex.head(10)
    print(len(dfHex),' ------  ',dfHex.head(10))

    dfHex=Get_Facebook_Population(province, dfHex, 'population')
    dfHex=Get_Facebook_Population_Youth_15_24(province, dfHex, 'population_youth')
    dfHex=Get_Facebook_Population_elderly_60_plus(province, dfHex, 'population_elder')
    dfHex=Get_Facebook_Population_children_under_five(province, dfHex, 'population_under_five')
    dfHex['population_515_2560']=dfHex['population']-dfHex['population_youth']-dfHex['population_elder']-dfHex['population_under_five']

    dfHex=Get_Facebook_Population_men(province, dfHex, 'population_men')
    dfHex=Get_Facebook_Population_women(province, dfHex, 'population_women')


    # Save dfHex with 2 ++ columns, hex_id and population and ++ in dfDummy
    dfDummy=dfHex.copy().reset_index(drop=True)

    # Find center point of each hex_id
    dfDummy['lat'] = dfDummy['hex_id'].apply(lambda x: h3.h3_to_geo(x)[0])
    dfDummy['lng'] = dfDummy['hex_id'].apply(lambda x: h3.h3_to_geo(x)[1])


    # Cretae geometry columns (this is in 4326 to use with Kepler.gl)
    dfDummy["geometry"] =  dfDummy.hex_id.apply(lambda x: 
                                                        {    "type" : "Polygon",
                                                                "coordinates": 
                                                                [h3.h3_to_geo_boundary(x)]                                                                
                                                            })

    ## csv file as an input of the ConvertCSV_To_Shapefile_rev2 on local machine
    print(' ===>  Kepler data out')
    dfDummy.rename(columns={'lat':'Latitude','lng':'Longitude'}, inplace=True)
    dfDummy.to_csv(write_path+'\\test_'+province+'_shapefile.csv')  
    dfDummy['p_name_t']=province
    dfDummy['DBCreatedAt']=nowStr 
    dfDummy_2=dfDummy.copy()
    dfDummy_2['geometry']=dfDummy_2['geometry'].astype(str)
    Write_H3_Kepler_Grid_Lv9_Province_2(dfDummy_2,conn)


    # Convert information in geometry columns to 32647 coordinates to use with QGIS
    print(' ===>  Convert Coordinates 4236 -> 32647 ')
    dfDummy['geom_2']=dfDummy.apply(lambda x: ConvertGeometryCoordinate(x['geometry']),axis=1 )
    dfDummy.drop(columns=['geometry'],inplace=True)
    dfDummy.rename(columns={'geom_2':'geometry'},inplace=True)
    dfDummy['geometry']=dfDummy['geometry'].astype(str)
    

    # Find District and Subdistrict location of each grid
    print(' ===>  Reverse Geocoding')
    dfPAT=Reverse_GeoCoding(dfDummy[['hex_id', 'Latitude', 'Longitude']])
    includeList=['hex_id', 'Latitude', 'Longitude','p_name_t',
       'a_name_t',  't_name_t', 's_region', 'prov_idn', 'amphoe_idn', 'tambon_idn']       
    dfPAT=dfPAT[includeList]    

    mainDf=pd.merge(dfDummy, dfPAT, how="left", on=["hex_id"])
    mainDf.rename(columns={'Latitude_x':'Latitude','Longitude_x':'Longitude','p_name_t_x':'p_name_t'},inplace=True)
    print(mainDf.columns,' ===> ',mainDf, ' ----  ',mainDf.dtypes)    

    # Write data to files and databse
    print(' ===>  QGIS data out ')
    mainDf.to_csv(qgis_path+'test_'+province+'_shapefile_32647_PAT.csv')   
    Write_H3_Grid_Lv9_Province_PAT(mainDf,conn) 

    # write temp file to check if the run is complete
    completeList.append(file_name)
    dfComplete=pd.DataFrame(completeList, columns=['Province'])
    if(len(completeList)==len(filenameList)):
        dfComplete['completeFlg']=1
    else:
        dfComplete['completeFlg']=0
    dfComplete.to_csv(temp_path+'continue.csv')
    del dfComplete




conn.close()
del dfDummy, dfHex, dfDummy_2, mainDf, dfPAT
del includeList, hexagons, totalList, testlist, hexList, filenameList, previousCompleteList
###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')