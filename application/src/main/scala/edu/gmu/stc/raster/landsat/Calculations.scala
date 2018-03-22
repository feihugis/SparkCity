package edu.gmu.stc.raster.landsat

import geotrellis.raster._

import scala.math._

/**
  * Object that can calculate ndvi and ndwi
  */
object Calculations {
  /** Calculates the normalized difference vegetation index
    * @param r value of red band
    * @param ir value of infra-red band
    */
  def ndvi (r: Double, ir: Double) : Double = {
    if (isData(r) && isData(ir)) {
        (ir - r) / (ir + r)
    } else {
      Double.NaN
    }
  }

  /** Calculates the normalized difference water index
    * @param g value of green band
    * @param ir value of infra-red band
    */
  def ndwi (g: Double, ir: Double) : Double = {
    if (isData(g) && isData(ir)) {
      (g - ir) / (g + ir)
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
      val lse = landsurfaceemissivity(r, ir, ndvi_min, ndvi_max)
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

  def landsurfaceemissivity(r: Double, ir: Double, ndvi_min: Double, ndvi_max: Double): Double = {
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
