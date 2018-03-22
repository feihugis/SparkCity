package edu.gmu.stc.raster.landsat

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.Geometry
import geotrellis.raster.DoubleConstantNoDataCellType
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.render.{ColorMap, ColorRamp, ColorRamps}
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.util.StreamingByteReader
import geotrellis.vector.{Extent, Polygon}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wololo.geojson.{Feature, FeatureCollection}
import org.wololo.jts2geojson.GeoJSONWriter

import scala.collection.JavaConverters._
import geotrellis.raster.ArrayMultibandTile
import geotrellis.raster.io.geotiff.MultibandGeoTiff


/**
  * Created by Fei Hu on 3/21/18.
  */
object CreateLSTPng {

  val maskedPath = "data/r-g-nir.tif"
  val lstPath = "data/lst-dc-2.png"

  def testHDFS = {
    val lst = {
      val geotiff = GeoTiffReader.readMultiband(
        StreamingByteReader(HdfsRangeReader(new Path(maskedPath),
          new Configuration())))


      val tile = geotiff.tile.convert(DoubleConstantNoDataCellType)


      println(geotiff.getClass)
      println(tile.getClass)

      val (ndvi_min, ndvi_max) = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
        MaskBandsRandGandNIR.NIR_BAND,
        MaskBandsRandGandNIR.TIRS_BAND) {
        (r: Double, ir: Double, tirs: Double) => Calculations.ndvi(r, ir);
      }.findMinMaxDouble

      tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
        MaskBandsRandGandNIR.NIR_BAND,
        MaskBandsRandGandNIR.TIRS_BAND) {
        (r: Double, ir: Double, tirs: Double) => Calculations.lst(r, ir, tirs, ndvi_min, ndvi_max);
      }
    }

    println(lst.getClass)

    val (min, max) = lst.findMinMaxDouble

    //val lst_new = lst.normalize(min, max, 0.0, 1.0).>(0.6)

    // Get color map from the application.conf settings file.
    //val colorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.lstColormap")).get
    //val colors: Vector[Int] = Vector(0x0000FF, 0xFF0000)
    //val ramp: ColorRamp = ColorRamp(colors)

    println("Rendering PNG and saving to disk...")
    val stops = 10
    //val breaks = for (i <- 1 to stops) yield i.toDouble / stops
    val breaks = for (i <- 0 to 2 * (max - min + 1).toInt) yield i*0.5 + min
    lst.renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum.toColorMap(breaks.toArray)).write(lstPath)
    //lst_new.renderPng(ramp.stops(stops).toColorMap(breaks.toArray)).write(lstPath)
    //lst.renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum.toColorMap(breaks.toArray)).write(lstPath)
    //lst_new.renderPng(colorMap).write(lstPath)
  }

  def vectorize = {
    val threshold = 3101111.0
    val lst = {
      val tile = MultibandGeoTiff(maskedPath).tile.convert(DoubleConstantNoDataCellType)
      println(tile.getClass)
      val (ndvi_min, ndvi_max) = tile.combineDouble(MaskBandsRandGandNIR.R_BAND, MaskBandsRandGandNIR.NIR_BAND, MaskBandsRandGandNIR.TIRS_BAND) { (r: Double, ir: Double, tirs: Double) =>
        Calculations.ndvi(r, ir);
      }.findMinMaxDouble

      tile.combineDouble(MaskBandsRandGandNIR.R_BAND, MaskBandsRandGandNIR.NIR_BAND, MaskBandsRandGandNIR.TIRS_BAND) { (r: Double, ir: Double, tirs: Double) =>
        Calculations.lst(r, ir, tirs, ndvi_min, ndvi_max)
      }
    }//.>(threshold)

    println(lst.getClass)

    val (min, max) = lst.findMinMaxDouble

    println(min, "*******", max)

    //val lst_new = lst.normalize(min, max, 0.0, 1.0).>(0.6)

    println(lst.dimensions)

    val vector = lst.toVector(new Extent(0, 0, 7831, 7951))
      .map(feature => feature.reproject((x, y) => (220800 + x*30, 4188000 + y*30)))
    //val vector = lst_new.toVector(new Extent(220800, 4188000, 455700, 4426500))


    println(vector.size)

    val vector_filered = vector.map(feature => {
      val polygon = feature.jtsGeom
      polygon.setUserData(feature.data)
      (feature.data, polygon)
    })
      .filter(pair => pair._1 > 300)
      .map(pair => pair._2)

    println(vector_filered.size)

    //val geojsonContent = vector.map(feature => feature.geom).toGeoJson
    val geojsonContent = toGeoJSON(vector_filered.toIterator)



    val file = new File(threshold + "_vectorize_categorized.geojson")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(geojsonContent)
    bw.close()
  }

  def toGeoJSON(iterator: Iterator[Geometry]): String = {
    val geoJSONWriter = new GeoJSONWriter
    val featureList = iterator.map(geometry => {
      if (geometry.getUserData != null) {
        val userData = Map("UserData" -> geometry.getUserData)
        val feature = new Feature(geoJSONWriter.write(geometry), userData.asJava)
        feature.setType("FeatureCollection")
        feature
      } else {
        val feature = new Feature(geoJSONWriter.write(geometry), null)
        feature.setType("FeatureCollection")
        feature
      }
    }).toList


    val featureCollection = new FeatureCollection(featureList.toArray[Feature])
    featureCollection.toString
  }



  def createLSTPng(args: Array[String]): Unit = {
    val lst = {
      // Convert the tile to type double values,
      // because we will be performing an operation that
      // produces floating point values.
      println("Reading in multiband image...")
      val tile = MultibandGeoTiff(maskedPath).tile.convert(DoubleConstantNoDataCellType)

      // Use the combineDouble method to map over the red and infrared values
      // and perform the NDVI calculation.
      println("Performing LST calculation...")
      val (ndvi_min, ndvi_max) = tile.combineDouble(MaskBandsRandGandNIR.R_BAND, MaskBandsRandGandNIR.NIR_BAND, MaskBandsRandGandNIR.TIRS_BAND) { (r: Double, ir: Double, tirs: Double) =>
        Calculations.ndvi(r, ir);
      }.findMinMaxDouble


      tile.combineDouble(MaskBandsRandGandNIR.R_BAND, MaskBandsRandGandNIR.NIR_BAND, MaskBandsRandGandNIR.TIRS_BAND) { (r: Double, ir: Double, tirs: Double) =>
        Calculations.lst(r, ir, tirs, ndvi_min, ndvi_max);
      }
    }


    //lst.foreachDouble((x, y, v) => println(x, y, v))


    val (min, max) = lst.findMinMaxDouble

    println(min, "*******", max)

    val lst_new = lst.normalize(min, max, 0.0, 1.0).>(0.6)

    // Get color map from the application.conf settings file.
    val colorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.lstColormap")).get
    val colors: Vector[Int] = Vector(0x0000FF, 0xFF0000)
    val ramp: ColorRamp = ColorRamp(colors)



    // Render this NDVI using the color breaks as a PNG,
    // and write the PNG to disk.
    println("Rendering PNG and saving to disk...")
    val stops = 10
    //val breaks = for (i <- 1 to stops) yield i.toDouble / stops
    val breaks = for (i <- 0 to 2 * (max - min + 1).toInt) yield i*0.5 + min
    //lst_new.renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum.toColorMap(breaks.toArray)).write(lstPath)
    //lst_new.renderPng(ramp.stops(stops).toColorMap(breaks.toArray)).write(lstPath)
    //lst.renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum.toColorMap(breaks.toArray)).write(lstPath)
    lst_new.renderPng(colorMap).write(lstPath)
  }

  def main(args: Array[String]): Unit = {
    //createLSTPng(args)
    //vectorize
    testHDFS
  }

}
