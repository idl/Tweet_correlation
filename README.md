#Tweet_correlation
Utilizes OGR/GDAL to check if a geo-coded tweet is contained within a polygon. The example data-set contains US counties shape file (polygons for each county) and a csv of twitter geo-coded data. Example run calculates the county fips code for each tweet and outputs in a csv file.

## Requirements
1. [GDAL](https://pypi.python.org/pypi/GDAL/)
2. Multiprocessing


## Utilization

```
python apply_polygon.py <shape_file> <shape_output_field> <input_csv_file> <output_csv_file>
```
Example: The test data consists of tweets from US and shape files for US counties
```
python apply_polygons.py ./Example_data_spanish/uscounties/uscounties.shp fips ./Example_data_spanish/output.csv test.csv
```

Input file format (input_csv_file):

ID  | Geo | Tweet | Created Time | User 
--- | --- | ----- | ------------ | ----
1234  | [Lat,Long] | Hi there | 2013-03-01T00:00:25.000Z | Test_user
