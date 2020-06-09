Example to load [New York Citiy Taxi Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) in an EXASOL database.

### Prerequisites:
-   EXASOL database, that is allowed to connect to the Internet and that has access to a nameserver
-   EXAplus (or any other SQL client)
-  [Exasol Query Wrapper](https://raw.githubusercontent.com/EXASOL/etl-utils/master/query_wrapper.sql ) in your database 

### Importing the dataset
Run the sql files in the following order

-   1_create_schema.sql
-   2_create_tables.sql
-   3_create_scripts.sql
-   4_import_data.sql

Scripts are implemented in a way, that a delta load should be easily possible in case new data gets published (as long as the format stays the same).

### How it works and what it does
On a very basic level this example is nothing but a CSV import via a staging layer. To make the dataset more useful and enrich the data the [load script](3_create_scripts.sql) does some manipulations during the loading process which will be discussed further.

#### The TRIPS_LOAD script
We use the [Exasol Query Wrapper](https://raw.githubusercontent.com/EXASOL/etl-utils/master/query_wrapper.sql) to parametrize the script which can make it hard to read at times but makes working with the script very easy and maintenance trivial.

This is a rough outline of what is happening in the script:

 1. Any tables that could interfere with the loading process are truncated. If there is a temporary stage table for the current file already, it is deleted.
 2. The current CSV file is loaded into a temporary staging table. The columns can differ and are handed to the script when it is called at the bottom of this file.
 3. The contents of the temporary stage table are transferred into the stage table. Information about the `vendor_type` and the `file_id` are added.
 4. The temporary stage table is dropped.
 5. If the columns `dropoff` and `pickup` columns contains longitude and latitude values they are converted to geometric `POINT()'s`.
 6. All `id's` are copied from the staging table to the `trip_location_id_map` where `dropoff_locationid` or `pickup_locationid` is `NULL`. This makes sure only rows are copied where the source data does not yet feature a location ID and further manipulation is necessary. This step is technically optional and could be integrated in the following `MERGE`-Command but clears up the ETL process quite a bit and makes it more readable.
 7. A `MERGE` statement is used to find out for each taxi trip in the stage table in which taxi zone it started and ended. An analytical function is used to make sure only one zone will be selected in case of an overlap between zones (a point is in two zones at the same time).
 8. The manipulated staging table is moved into the prod table. Further manipulations takes place in this step. For example ID-References are created and regular expressions are used to streamline varchar fields with different spelling. 
 9. The `RAW_DATA_URLS` table gets a timestamp to mark the current file as successfully loaded.

Following the script definition are the script calls. The script call is parametrized and fitted to each input file format. Since file formats changed a lot over the past years, a lot of different script calls are necessary.

#### A note on GEOSPATIAL data
The taxi zones are stored as high detail polygons: <br>
![A NYC Taxi polygon](https://raw.githubusercontent.com/exasol/opendata-examples/LN_update_11_2019/nyc_taxi/2020-06-04%2012_26_20-DBeaver%207.1.0%20-%20TAXI_ZONES.png "A NYC Taxi polygon")


To check if a point resides within this polygon is a very resource intensive task. In the ETL process the polygon is therefore broken up into pieces:

 1. We look for the smallest rectangle the polygon fits in:  `ST_ENVELOPE`
 2. We take the `ST_BOUNDARY`of this rectangle which only consists of the four corners of the rectangle
 3. We take the `ST_X` and `ST_Y` coordinates of the `ST_POINTN`'s 2 and 4 (upper left and lower right corner) of the `ST_BOUNDARY` of the rectangle and pass it to the `create_polygon_grid` [script](3_create_scripts.sql).
 4. The script calculates a grid which can be laid over the polygon. Default size is 10x10 but we use 25x25 for even finer segments.
 5. With the polygon and the grid we now cut away anything from the grid that is not part of the polygon which results in a segmented version of the polygon: 
 
![A segmented NYC taxi polygon](https://raw.githubusercontent.com/exasol/opendata-examples/LN_update_11_2019/nyc_taxi/2020-06-04%2016_03_48-DBeaver%207.1.0%20-%20SPATIAL_GRID_MERGE.png)

Instead of searching in one big polygon we can now search in a lot of small polygons which is significantly (in this case 10x) faster.
