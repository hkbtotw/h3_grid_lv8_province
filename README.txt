#Script to generate shapefile of grids on each province

Update:
-Created on 2021-06-10


Why:
- Goal at the creation of this script is the create refined grids finer than the sub district area to present the population density obtained form facebook which comes in points of lat and lng.

What:
- Use Uber - h3 and Geopandas to extract boundary of each province from QGIS's shapefile of Thailand provinces; then, pass then on to Uber's H3 for griding 


How:
1.Run Inegreated_Shapefile_Generation.py

Note: Code will read the boundary data file in folder boundary and generate shapefiles ( in csv ) to use with Kepler.gl and QGIS 
in table : [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province]  and [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province] respectively.
=> Code uses h8-level grid which covers around 0.7 km2 on the map