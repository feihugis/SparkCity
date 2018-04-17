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
    
 * Launch Spark-shell:
 ```shell
  ~/spark-2.3.0-bin-hadoop2.6/bin//spark-shell --master yarn --deploy-mode client --num-executors 10 --driver-memory 12g --executor-memory 10g --executor-cores 24 --jars ~/SparkCity/application/target/sparkcity-application-1.1.0-SNAPSHOT.jar
 ```
 * Combine multiple bands
 ```scala
 import edu.gmu.stc.analysis.MaskBandsRandGandNIR
 val input = "/SparkCity/data/LC08_L1TP_015033_20170822_20170822_01_RT/LC08_L1TP_015033_20170822_20170822_01_RT"
 val output = "/SparkCity/data/LC08_L1TP_015033_20170822_20170822_01_RT/LC08_L1TP_015033_20170822_20170822_01_RT_r-g-nir-tirs1-swir1-test.tif"
 MaskBandsRandGandNIR.combineBands(input, output)
 ```
 ```scala
 import edu.gmu.stc.analysis.MaskBandsRandGandNIR
 val landsatTxtPath = "/SparkCity/landsat_hdfs"
 MaskBandsRandGandNIR.combineBands(sc, landsatTxtPath)
 ```
 
 * Build index
 ```scala
 import edu.gmu.stc.analysis.BuildShapeFileIndex
 val hconf = "/var/lib/hadoop-hdfs/SparkCity/config/conf_lst_cluster.xml"
 val inputDir = "/SparkCity/data/"
 BuildShapeFileIndex.buildIndex(sc, hconf, inputDir + "/dc")
 BuildShapeFileIndex.buildIndex(sc, hconf, inputDir + "/va")
 BuildShapeFileIndex.buildIndex(sc, hconf, inputDir + "/md")
 ```

 * Compute SpectralIndex
```scala 
import edu.gmu.stc.analysis.ComputeSpectralIndex
val landsatTiff = "/SparkCity/data/LC08_L1TP_015033_20170416_20170501_01_T1/LC08_L1TP_015033_20170416_20170501_01_T1_r-g-nir-tirs1-swir1.tif"
val time = "20170416"
val outputDir = "SparkCity/data"
val hConfFile = "/var/lib/hadoop-hdfs/SparkCity/config/conf_lst_cluster.xml"
ComputeSpectralIndex.computeInCluster(sc, landsatTiff, time, outputDir, hConfFile)
```


 
 


## Resources
 * [FRAGSTATS: Spatial Pattern Analysis Program for Categorical Maps](https://www.umass.edu/landeco/research/fragstats/fragstats.html) 
 * [Atmospheric Correctin parameter Calculator](https://atmcorr.gsfc.nasa.gov/)


## Computing
 * LST for different landuse type
 * Locations for highest-temperature area
 