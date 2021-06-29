<h1>#Script to generate shapefile of grids on each province </h1>

Update: <br />
2021-06-10 Created by Tawan, T. <br />
2021-06-28 Modify to handle incomplete runs <br />


Why:<br />
- Goal at the creation of this script is the create refined grids finer than the sub district area to present the population density obtained form facebook which comes in points of lat and lng.

What:<br />
- Use Uber - h3 and Geopandas to extract boundary of each province from QGIS's shapefile of Thailand provinces; then, pass then on to Uber's H3 for griding 

Avail Dataset:<br />
1.General population<br />
2.Youth population   (15-24)<br />
3.Elder population  (>60)<br />
4.Under five population<br />
5.Men population<br />
6.Women population<br />


How:<br />
1.Select h3_level to generate h3 grids  <br />
    h3_level=9   

  Select if using the specific provinces<br />
    # if_all_provinces=1 => Use all provinces in boundary_data<br />
    # if_all_provinces=2 => Incase, previous run not complete, Continue running from what being left off from previous run.<br />
    if_all_provinces=1<br />

2.Run Inegreated_Shapefile_Generation.py<br />


Note: Code will read the boundary data file in folder boundary and generate shapefiles ( in csv ) to use with Kepler.gl and QGIS <br />
in table : [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2]  and [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT] respectively.<br />
=> Code uses h8-level grid which covers around 0.7 km2 on the map<br />

tutorial on H3:<br />
1.Study ipython files in ipython_notebook folder  (to run use Folium environment and install kernel in jupyter notebook)<br />



Note:<br />
1.Facebook population density: Estimation of numbe of people in each 30 metres grid around the world<br />
2.Data could be obtained from  data.humdata.org<br />

ref: <br />
1. https://dataforgood.fb.com/docs/high-resolution-population-density-maps-demographic-estimates-documentation/    (general information of fb population density dataset)<br />
2. https://data.humdata.org/organization/facebook   (download data)<br />
3. https://h3geo.org/docs/api/traversal#hexring  (H3 API documentation)<br />

DEMO <br />
H3's grid Level 8 of BKK <br />
<img src=https://github.com/hkbtotw/h3_grid_lv8_province/blob/master/BKK_Lv8_h3.JPG alt="Demo h3's grids Level 8 of BKK" width="800"/>

H3's grid Level 9 of BKK <br />
<img src=https://github.com/hkbtotw/h3_grid_lv8_province/blob/master/BKK_Lv9_h3.JPG alt="Demo h3's grids Level 9 of BKK" width="800"/>