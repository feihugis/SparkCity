# Spark-based Framework for Big GeoSpatial Vector Data Analytics

This repository is developping a Spark-based framework for big geospatial vector data analytics
using GeoSpark.

## Example
 * edu.gmu.stc.vector.examples.GeoSparkExample


## Notes
 * Maven pom file dependence: the difference between `provider` and `compile`(default) of the scope value
 * Compile jars: `mvn clean install -DskipTests=true`
 * Connect PostgreSQL: ` psql -h 10.192.21.133 -p 5432 -U postgres -W`
 * Run Spark-shell: `spark-shell --master yarn --deploy-mode client 
   --jars /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar --num-executors 5 --driver-memory 12g 
   --executor-memory 10g --executor-cores 22`
 * Submit Spark-jobs: 
    - Available GridType: `EQUALGRID`, `HILBERT`, `RTREE`, `VORONOI`, `QUADTREE`, `KDBTREE`
    - Available IndexType: `RTREE`, `QUADTREE`
    - GeoSpark: `spark-submit --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 22 --class edu.gmu.stc.vector.sparkshell.OverlapPerformanceTest /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar /user/root/data0119/ Impervious_Surface_2015_DC Soil_Type_by_Slope_DC Partition_Num GridType IndexType`
    - STCSpark_Build_Index: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar BuildIndex /home/fei/GeoSpark/config/conf.xml`
    - STCSpark_Overlap_v1: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar /home/fei/GeoSpark/config/conf.xml Partition_Num GridType IndexType /test.geojson`
    - STCSpark_Overlap_v2: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v2 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar /home/fei/GeoSpark/config/conf.xml Partition_Num GridType IndexType /test.geojson`
    - STCSpark_Overlap_v3: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v3 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar /home/fei/GeoSpark/config/conf.xml Partition_Num GridType IndexType /test.geojson`
    