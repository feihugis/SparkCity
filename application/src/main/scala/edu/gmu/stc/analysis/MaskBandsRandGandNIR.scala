package edu.gmu.stc.analysis

import edu.gmu.stc.analysis.MaskBandsRandGandNIR.bandPath
import edu.gmu.stc.hdfs.HdfsUtils
import edu.gmu.stc.raster.io.GeoTiffReaderHelper
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

object MaskBandsRandGandNIR {
  val maskedPath = "data/landsat8_dc/LC08_L1TP_015033_20170416_20170501_01_T1/r-g-nir-tirs1-swir1-test.tif"
  // Path to our landsat band geotiffs.
  def bandPath(b: String) = s"data/landsat8_dc/LC08_L1TP_015033_20170416_20170501_01_T1/LC08_L1TP_015033_20170416_20170501_01_T1_${b}.TIF"
  //constants to differentiate which bands to use
  val R_BAND = 0
  val G_BAND = 1
  val NIR_BAND = 2
  val TIRS1_BAND = 3
  val SWIR1_BAND = 4

  def combineBands(bandPathPrefix: String, output: String): Unit = {
    def bandPath(b: String) = bandPathPrefix + s"_${b}.TIF"
    val conf = new Configuration()
    // Read in the red band
    println("Reading in the red band...")
    val rGeoTiff = GeoTiffReaderHelper.readSingleBand(new Path(bandPath("B4")), conf)

    // Read in the green band
    println("Reading in green band...")
    val gGeoTiff = GeoTiffReaderHelper.readSingleBand(new Path(bandPath("B3")), conf)

    // Read in the near infrared band (NIR)
    println("Reading in the NIR band...")
    val nirGeoTiff = GeoTiffReaderHelper.readSingleBand(new Path(bandPath("B5")), conf)

    // Read in the TIRS1 (band10) band
    println("Reading in the TIRS band/band10")
    val tirs1GeoTiff = GeoTiffReaderHelper.readSingleBand(new Path(bandPath("B10")), conf)

    // Read in the TIRS1 (band10) band
    println("Reading in the SWIR1")
    val swir1GeoTiff = GeoTiffReaderHelper.readSingleBand(new Path(bandPath("B6")), conf)

    // Read in the QA band
    println("Reading in the QA band...")
    val qaGeoTiff = GeoTiffReaderHelper.readSingleBand(new Path(bandPath("BQA")), conf)

    // GeoTiffs have more information we need; just grab the Tile out of them.
    val (rTile, gTile, nirTile, qaTile, tirs1Tile, swir1Tile) = (rGeoTiff.tile, gGeoTiff.tile,
      nirGeoTiff.tile, qaGeoTiff.tile, tirs1GeoTiff.tile, swir1GeoTiff.tile)

    // This function will set anything that is potentially a cloud to NODATA
    def maskClouds(tile: Tile): Tile =
      tile.combine(qaTile) { (v: Int, qa: Int) =>
        val isCloud = qa & 0x8000
        val isCirrus = qa & 0x2000
        if(isCloud > 0 || isCirrus > 0) { NODATA }
        else { v }
      }

    // Mask our red, green and near infrared bands using the qa band
    println("Masking clouds in the red band...")
    val rMasked = maskClouds(rTile)
    println("Masking clouds in the green band...")
    val gMasked = maskClouds(gTile)
    println("Masking clouds in the NIR band...")
    val nirMasked = maskClouds(nirTile)
    println("Masking clouds in the TIRS band...")
    val tirs1Masked = maskClouds(tirs1Tile)
    println("Masking clouds in the SWIR1 band...")
    val swir1Masked = maskClouds(swir1Tile)

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rMasked, gMasked, nirMasked, tirs1Masked, swir1Masked).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    println("Writing out the multiband R + G + NIR + TIRS1 + SWIR1 tile...")
    // MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).write(maskedPath)
    val byteArray = MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).toByteArray
    HdfsUtils.write(new Path(output), new Configuration(), byteArray)
  }

  def combineBands(sc: SparkContext, landsatTxtPath: String): Unit = {
    val paths = sc.textFile(landsatTxtPath)
    val suffix = "_r-g-nir-tirs1-swir1.tif"
    paths.foreach(landsatPre => combineBands(landsatPre, landsatPre + suffix))
  }

  def main(args: Array[String]): Unit = {
    // Read in the red band
    println("Reading in the red band...")
    val rGeoTiff = SinglebandGeoTiff(bandPath("B4"))

    // Read in the green band
    println("Reading in green band...")
    val gGeoTiff = SinglebandGeoTiff(bandPath("B3"))

    // Read in the near infrared band (NIR)
    println("Reading in the NIR band...")
    val nirGeoTiff = SinglebandGeoTiff(bandPath("B5"))

    // Read in the TIRS1 (band10) band
    println("Reading in the TIRS band/band10")
    val tirs1GeoTiff = SinglebandGeoTiff(bandPath("B10"))

    // Read in the TIRS1 (band10) band
    println("Reading in the SWIR1")
    val swir1GeoTiff = SinglebandGeoTiff(bandPath("B6"))

    // Read in the QA band
    println("Reading in the QA band...")
    val qaGeoTiff = SinglebandGeoTiff(bandPath("BQA"))

    // GeoTiffs have more information we need; just grab the Tile out of them.
    val (rTile, gTile, nirTile, qaTile, tirs1Tile, swir1Tile) = (rGeoTiff.tile, gGeoTiff.tile,
      nirGeoTiff.tile, qaGeoTiff.tile, tirs1GeoTiff.tile, swir1GeoTiff.tile)

    // This function will set anything that is potentially a cloud to NODATA
    def maskClouds(tile: Tile): Tile =
      tile.combine(qaTile) { (v: Int, qa: Int) =>
        val isCloud = qa & 0x8000
        val isCirrus = qa & 0x2000
        if(isCloud > 0 || isCirrus > 0) { NODATA }
        else { v }
      }

    // Mask our red, green and near infrared bands using the qa band
    println("Masking clouds in the red band...")
    val rMasked = maskClouds(rTile)
    println("Masking clouds in the green band...")
    val gMasked = maskClouds(gTile)
    println("Masking clouds in the NIR band...")
    val nirMasked = maskClouds(nirTile)
    println("Masking clouds in the TIRS band...")
    val tirs1Masked = maskClouds(tirs1Tile)
    println("Masking clouds in the SWIR1 band...")
    val swir1Masked = maskClouds(swir1Tile)

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rMasked, gMasked, nirMasked, tirs1Masked, swir1Masked).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    println("Writing out the multiband R + G + NIR + TIRS1 + SWIR1 tile...")
    // MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).write(maskedPath)
    val byteArray = MultibandGeoTiff(mb, rGeoTiff.extent, rGeoTiff.crs).toByteArray
    HdfsUtils.write(new Path(maskedPath), new Configuration(), byteArray)
  }
}
