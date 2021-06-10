from h3 import h3
from Database_Population import *
from datetime import datetime, date, timedelta
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from Credential import *
import numpy as np
import os
import ast
import pandas as pd
import pickle
import glob

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

def Write_H3_Grid_Province(df_input):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn1 = connect_tad
    

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
    conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Province(df_input):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn2 = connect_tad_2
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

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
    conn2.close()
    print('------------Complete WriteDB-------------')




file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\boundary_data\\'
write_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\shapefile\\'
qgis_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Uber_h3\\qgis_shapefile\\'

# Read province boundary data
filenameList=GetFileNameList(file_path)
#print(' ---> ',filenameList)

for file_name in filenameList:  # [:2]:
    #file_name='boundary_ชลบุรี.data'

    province=file_name.split('_')[1].split('.')[0]
    print(' - ',province)

    with open(file_path+file_name,'rb') as filehandle:
        testlist=pickle.load(filehandle)

    hexList=[]
    for coor in testlist:  
        geoJson = {"coordinates": [coor], "type": "Polygon"}
        # level 8 covers approx 1 km2
        h3_level=8        
            
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

    dfIn=Read_Location_Popoulation(province)

    if(len(dfIn)>0):
        print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)
        
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()

        dfHex=pd.DataFrame(hexagons, columns=['hex_id'])
        #dfHex.head(10)
        print(len(dfHex),' ------  ',dfHex.head(10))

        dfHex['population']=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))

        dfDummy=dfHex.copy().reset_index(drop=True)

        dfDummy['lat'] = dfDummy['hex_id'].apply(lambda x: h3.h3_to_geo(x)[0])
        dfDummy['lng'] = dfDummy['hex_id'].apply(lambda x: h3.h3_to_geo(x)[1])

        # write out lat, lng, population, hex_id  of all grids generated
        #dfDummy.to_csv(file_path+'test_'+province+'_h3.csv')

        dfDummy["geometry"] =  dfDummy.hex_id.apply(lambda x: 
                                                            {    "type" : "Polygon",
                                                                    "coordinates": 
                                                                    [h3.h3_to_geo_boundary(x)]
                                                                    
                                                                })

        ## csv file as an input of the ConvertCSV_To_Shapefile_rev2 on local machine
        dfDummy.rename(columns={'lat':'Latitude','lng':'Longitude'}, inplace=True)
        dfDummy.to_csv(write_path+'\\test_'+province+'_shapefile.csv')  
        dfDummy['p_name_t']=province
        dfDummy['DBCreatedAt']=nowStr 
        dfDummy_2=dfDummy.copy()
        dfDummy_2['geometry']=dfDummy_2['geometry'].astype(str)
        Write_H3_Kepler_Grid_Province(dfDummy_2)

        
        #print(' --> ',df.head(10))
        # 4 Create tuples of geometry by zipping Longitude and latitude columns in your csv file
        #geometry = [Point(xy) for xy in zip(df.Longitude, df.Latitude)]         
        #print(' ===> ',geometry)

        dfDummy['geom_2']=dfDummy.apply(lambda x: ConvertGeometryCoordinate(x['geometry']),axis=1 )
        dfDummy.drop(columns=['geometry'],inplace=True)
        dfDummy.rename(columns={'geom_2':'geometry'},inplace=True)
        dfDummy['geometry']=dfDummy['geometry'].astype(str)

        print(dfDummy.columns,' ===> ',dfDummy)
        dfDummy.to_csv(qgis_path+'test_'+province+'_shapefile_32647.csv')   
        Write_H3_Grid_Province(dfDummy)    
    else:
        print(' No population in =======> ',province)

del dfDummy, dfHex, dfagg, dfDummy_2
###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')