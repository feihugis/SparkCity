package edu.gmu.stc.analysis

import com.vividsolutions.jts.geom._
import edu.gmu.stc.raster.io.GeoTiffReaderHelper
import edu.gmu.stc.raster.landsat.{Calculations, MaskBandsRandGandNIR}
import edu.gmu.stc.vector.VectorUtil
import edu.gmu.stc.vector.io.ShapeFileReaderHelper
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import geotrellis.raster.{ArrayMultibandTile, DoubleConstantNoDataCellType, Tile}
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 3/21/18.
  */
object ComputeLST {

  def computeLST(hConf: Configuration,
                 landsatMulBandFilePath: Path): (Extent, Tile) = {
    val geotiff = GeoTiffReaderHelper.readMultiband(landsatMulBandFilePath, hConf)
    val tile = geotiff.tile.convert(DoubleConstantNoDataCellType)
    val (ndvi_min, ndvi_max) = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
      MaskBandsRandGandNIR.NIR_BAND,
      MaskBandsRandGandNIR.TIRS_BAND) {
      (r: Double, ir: Double, tirs: Double) => Calculations.ndvi(r, ir);
    }.findMinMaxDouble


    val lstTile = tile.combineDouble(MaskBandsRandGandNIR.R_BAND,
      MaskBandsRandGandNIR.NIR_BAND,
      MaskBandsRandGandNIR.TIRS_BAND) {
      (r: Double, ir: Double, tirs: Double) => Calculations.lst(r, ir, tirs, ndvi_min, ndvi_max);
    }

    (geotiff.extent, lstTile)
  }

  def clipToPolygons(extent: Extent, tile: Tile, polygons: List[Geometry]): List[Geometry] = {
    polygons.map {
      case p: Polygon => {
        val meanValue = tile.polygonalMean(extent, p)
        p.setUserData(meanValue)
        p
      }
      case mp: MultiPolygon => {
        val meanValue = tile.polygonalMean(extent, mp)
        mp.setUserData(meanValue)
        mp
      }
      case s: Any => {
        println("***********************")
        println(s.getClass)
        s
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val landsatFilePath = new Path("/Users/feihu/Documents/GitHub/SparkCity/data/r-g-nir.tif")
    val hConf = new Configuration()
    hConf.addResource(new Path("/Users/feihu/Documents/GitHub/SparkCity/config/conf_lst_va.xml"))

    val sourceCRS = "epsg:32618"
    val targetCRS = "epsg:4269"
    val transform = VectorUtil.getCRSTransform(sourceCRS, targetCRS, true)

    val (extent, lstTile) = computeLST(hConf, landsatFilePath)
    println(extent.xmin, extent.ymin)

    val envelope = new Envelope(extent.xmin, extent.xmax, extent.ymin, extent.ymax)
    println(envelope)
    val bbox = JTS.transform(envelope, transform)
    println(bbox)

    val extentTransformed = new Extent(bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)

    val tableName = "gis_osm_landuse_a_free_1"
    val polygons = ShapeFileReaderHelper.read(hConf, tableName,
      bbox.getMinX,
      bbox.getMinY,
      bbox.getMaxX,
      bbox.getMaxY,
      true)

    val polygonsWithTemperature = clipToPolygons(extentTransformed, lstTile, polygons)

    GeometryReaderUtil.saveAsShapefile("/Users/feihu/Documents/GitHub/SparkCity/data/lst_landuse_va/lst_landuse_va.shp", polygonsWithTemperature.asJava, "epsg:4269")
  }



}
