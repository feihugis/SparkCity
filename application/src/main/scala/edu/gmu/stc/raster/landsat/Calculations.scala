package edu.gmu.stc.raster.landsat

import geotrellis.raster._

import scala.math._

/**
  * Object that can calculate ndvi and ndwi
  */
object Calculations {
  /** Calculates the normalized difference vegetation index
    * @param r value of red band
    * @param nir value of infra-red band
    */
  def ndvi (r: Double, nir: Double) : Double = {
    if (isData(r) && isData(nir)) {
        (nir - r) / (nir + r)
    } else {
      Double.NaN
    }
  }

  /** Calculates the normalized difference water index
    * @param g value of green band
    * @param nir value of infra-red band
    */
  def ndwi (g: Double, nir: Double) : Double = {
    if (isData(g) && isData(nir)) {
      (g - nir) / (g + nir)
    } else {
      Double.NaN
    }
  }

  /**
    * Calculate the normalized difference built-up index
    * @param swir1 Band 6 - Shortwave Infrared (SWIR) 1
    * @param nir Band 5 - Near Infrared (NIR)
    * @return
    */
  def ndbi(swir1: Double, nir: Double) : Double = {
    if (isData(swir1) && isData(nir)) {
      (swir1 - nir) / (swir1 + nir)
    } else {
      Double.NaN
    }
  }

  /**
    * Calculate the normalized difference imperious index
    * @param vis visible band (e.g. r band), such as band 2, 3, 4; We the choose the red (4) band
    *            here based on this paper, Wang, Z., Gang, C., Li, X., Chen, Y. and Li, J., 2015.
    *            Application of a normalized difference impervious index (NDII) to extract urban
    *            impervious surface features based on Landsat TM images. International Journal of
    *            Remote Sensing, 36(4), pp.1055-1069.
    * @param tir1 Band 10 - Thermal Infrared (TIRS) 1
    * @return
    */
  def ndii (vis: Double, tir1: Double): Double = {
    if (isData(vis) && isData(tir1)) {
      (vis - tir1) / (vis + tir1)
    } else {
      Double.NaN
    }
  }


  //Reference: Garg, A., Pal, D., Singh, H. and Pandey, D.C., 2016, November. A comparative study of
  // NDBI, NDISI and NDII for extraction of urban impervious surface of Dehradun [Uttarakhand, India]
  // using Landsat 8 imagery. In Emerging Trends in Communication Technologies (ETCT), International
  // Conference on (pp. 1-5). IEEE.

  /**
    * Calculate the water index
    * @param g Band 3 - Green
    * @param swir1 Band 6 - Shortwave Infrared (SWIR) 1
    * @return
    */
  def mndwi(g: Double, swir1: Double): Double = {
    if (isData(g) && isData(swir1)) {
      (g - swir1) / (g + swir1)
    } else {
      Double.NaN
    }
  }

  /**
    * Calculate the normalized difference impervious surface index
    * @param tir1 Band 10 - Thermal Infrared (TIRS) 1
    * @param g Band 3 - Green
    * @param nir Band 5 - Near Infrared (NIR)
    * @param swir1 Band 6 - Shortwave Infrared (SWIR) 1
    * @return
    */
  def ndisi(tir1: Double, g: Double, nir: Double, swir1: Double): Double = {
    if (isData(tir1) && isData(g) && isData(nir) && isData(swir1) && isData(swir1)) {
      val mndwi = (g - swir1) / (g + swir1)
      (tir1 - (mndwi + nir + swir1)/3) / (tir1 + (mndwi + nir + swir1)/3)
    } else {
      Double.NaN
    }
  }


  val Qmax = 65535.0
  val Qmin = 1.0
  val Lmax = 22.00180
  val Lmin = 0.10033

  def lst(r: Double, ir: Double, tirs: Double, ndvi_min: Double, ndvi_max: Double): Double = {
    if (tirs < Qmin) {
      return Double.NaN
    }

    if (isData(r) && isData(ir) && isData(tirs)) {
      val toa_val = toa(tirs)
      val brightnessTemp = brightnesstemperature(toa_val)

      val lamda = 11.5
      val p = 0.014388 * 1000000 //6.626 * 2.998 / 1.38 / pow(10.0, 3.0)
      val lse = landSurfaceEmissivity(r, ir, ndvi_min, ndvi_max)
      val land_surface_temperature = brightnessTemp/(1 + lamda * brightnessTemp / p * log(lse))

     /* if (land_surface_temperature.toString.equals("NaN")) {
        println(r, ir, tirs, toa_val, brightnessTemp, lse, land_surface_temperature)
      }*/

      land_surface_temperature.toInt.toDouble
    } else {
      Double.NaN
    }
  }

  def toa(tirs: Double): Double = {
    (Lmax - Lmin) / (Qmax - Qmin) * (tirs - Qmin) + Lmin
  }

  def brightnesstemperature(toa: Double): Double = {
    val k1 = 607.76
    val k2 = 1260.56

    k2/log(k1/toa + 1)
  }

  def landSurfaceEmissivity(r: Double, ir: Double, ndvi_min: Double, ndvi_max: Double): Double = {
    val ndvi_val = ndvi(r, ir)
    val pv = pow((ndvi_val - ndvi_min) / (ndvi_max - ndvi_min), 2.0)

    val tse = if (ndvi_val < 0.2) {
      0.979 - 0.035*r/100000.0
    } else if (ndvi_val <= 0.5) {
      0.986 + 0.004 * pv
    } else {
      0.99
    }

    tse
  }


}
