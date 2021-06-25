#Script to generate shapefile of grids on each province

Update:
-Created on 2021-06-10


Why:
- Goal at the creation of this script is the create refined grids finer than the sub district area to present the population density obtained form facebook which comes in points of lat and lng.

What:
- Use Uber - h3 and Geopandas to extract boundary of each province from QGIS's shapefile of Thailand provinces; then, pass then on to Uber's H3 for griding 

Avail Dataset:
1.General population
2.Youth population   (15-24)
3.Elder population  (>60)
4.Under five population
5.Men population
6.Women population


How:
1.Run Inegreated_Shapefile_Generation.py

Note: Code will read the boundary data file in folder boundary and generate shapefiles ( in csv ) to use with Kepler.gl and QGIS 
in table : [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2]  and [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT] respectively.
=> Code uses h8-level grid which covers around 0.7 km2 on the map

tutorial on H3:
1.Study ipython files in ipython_notebook folder  (to run use Folium environment and install kernel in jupyter notebook)



Note:
1.Facebook population density: Estimation of numbe of people in each 30 metres grid around the world
2.Data could be obtained from  data.humdata.org

ref: 
1. https://dataforgood.fb.com/docs/high-resolution-population-density-maps-demographic-estimates-documentation/    (general information of fb population density dataset)
2. https://data.humdata.org/organization/facebook   (download data)
3. https://h3geo.org/docs/api/traversal#hexring  (H3 API documentation)