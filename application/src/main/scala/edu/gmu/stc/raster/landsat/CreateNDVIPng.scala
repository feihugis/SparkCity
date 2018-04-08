package edu.gmu.stc.raster.landsat

import com.typesafe.config.ConfigFactory
import edu.gmu.stc.analysis.MaskBandsRandGandNIR
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._

object CreateNDVIPng {
  val maskedPath = "data/r-g-nir.tif"
  val ndviPath = "data/ndvi-dc.png"

  def main(args: Array[String]): Unit = {
    val ndvi = {
      // Convert the tile to type double values,
      // because we will be performing an operation that
      // produces floating point values.
      println("Reading in multiband image...")
      val tile = MultibandGeoTiff(maskedPath).tile.convert(DoubleConstantNoDataCellType)

      // Use the combineDouble method to map over the red and infrared values
      // and perform the NDVI calculation.
      println("Performing NDVI calculation...")
      tile.combineDouble(MaskBandsRandGandNIR.R_BAND, MaskBandsRandGandNIR.NIR_BAND) { (r: Double, ir: Double) =>
        Calculations.ndvi(r, ir);
      }
    }

    val (min, max) = ndvi.findMinMaxDouble

    println(min, "*******", max)

    // Get color map from the application.conf settings file.
    val colorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.ndviColormap")).get

    // Render this NDVI using the color breaks as a PNG,
    // and write the PNG to disk.
    println("Rendering PNG and saving to disk...")
    ndvi.renderPng(colorMap).write(ndviPath)
  }
}
