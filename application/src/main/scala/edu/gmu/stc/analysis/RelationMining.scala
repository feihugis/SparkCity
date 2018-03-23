package edu.gmu.stc.analysis


import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * Created by Fei Hu on 3/22/18.
  */
object RelationMining {

  def linearRegressionWithElasticNet(): Unit = {
    val spark = SparkSession
      .builder
      .master("local[6]")
      .appName(s"LinearRegression")
      .getOrCreate()

    // Load training data
    val training = spark.read.format("libsvm")
      .load("/Users/feihu/Documents/GitHub/SparkCity/data/lst_va_libsvm/part-00000-2b65712e-d9dd-4b6c-8f35-c2ffc8f874a1-c000.libsvm").limit(200)
      //.load("/Users/feihu/Documents/GitHub/SparkCity/data/sample_linear_regression_data.txt")

    /*val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val training = normalizer.transform(training_input)*/

    training.show()

    val lr = new LinearRegression()
      .setMaxIter(1000)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    linearRegressionWithElasticNet()
  }

}
