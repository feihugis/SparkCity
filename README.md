# Spark-based Framework for Smart City

This repository is developping a Spark-based framework to get better understanding of our cities 
by utilizing multiple-resource datasets, e.g. satellite images, geospatial vector data, and social 
media data.   


## Acknowlegement
 * This project leverages GeoSpark to process big vector datasets. But since their source code are
 revised to meet our specific requirements, their source code are put under this repository.  

## Notes
 * Maven pom file dependence: the difference between `provider` and `compile`(default) of the scope value
 * Compile jars: `mvn clean install -DskipTests=true`
 * Connect PostgreSQL: ` psql -h 10.192.21.133 -p 5432 -U postgres -W`
 * Run Spark-shell: `spark-shell --master yarn --deploy-mode client 
   --jars /root/AiCity/AiCity-application-1.1.0-SNAPSHOT.jar --num-executors 5 --driver-memory 12g 
   --executor-memory 10g --executor-cores 22`
 * Submit Spark-jobs: 
    - Available GridType: `EQUALGRID`, `HILBERT`, `RTREE`, `VORONOI`, `QUADTREE`, `KDBTREE`
    - Available IndexType: `RTREE`, `QUADTREE`
    - GeoSpark: `spark-submit --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 22 --class edu.gmu.stc.vector.sparkshell.OverlapPerformanceTest /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar /user/root/data0119/ Impervious_Surface_2015_DC Soil_Type_by_Slope_DC Partition_Num GridType IndexType`
    - STCSpark_Build_Index: `spark-shell --master yarn --deploy-mode client --num-executors 16 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest --jars application/target/sparkcity-application-1.1.0-SNAPSHOT.jar BuildIndex config/conf.xml`
    - STCSpark_Overlap_v1: `spark-shell --master yarn --deploy-mode client --num-executors 16 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest --jars application/target/sparkcity-application-1.1.0-SNAPSHOT.jar config/conf.xml Partition_Num GridType IndexType /test.geojson`
    - STCSpark_Overlap_v2: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v2 --jars application/target/sparkcity-application-1.1.0-SNAPSHOT.jar conf.xml Partition_Num GridType IndexType /test.geojson`
    - STCSpark_Overlap_v3: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v3 --jars application/target/sparkcity-application-1.1.0-SNAPSHOT.jar config/conf.xml Partition_Num GridType IndexType /test.geojson`
    - SpatialJoin: `spark-shell --master yarn --deploy-mode client --num-executors 16 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.SpatialJoin --jars application/target/sparkcity-application-1.1.0-SNAPSHOT.jar  config/conf.xml Partition_Num GridType IndexType`
    
    