/**
  * Created by oleg.baydakov on 27/07/2017.
  * https://github.com/h2oai/sparkling-water/blob/master/examples/src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo.scala
  */

import java.io.File

import org.apache.spark.h2o.{DoubleHolder, H2OContext, H2OFrame}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.examples.h2o._
import org.apache.spark.sql.{DataFrame, _}
import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation


object AirlinesWithWeatherDemo extends InitSpark {

  def addFiles(sc: SparkContext, files: String*): Unit = {
    files.foreach(f => sc.addFile(f))
  }

  def enforceLocalSparkFile(file: String): String ={
    "file://" + SparkFiles.get(file)
  }

  def absPath(path: String): String = new java.io.File(path).getAbsolutePath



  def main(args: Array[String]): Unit = {


    @transient val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    import spark.implicits._

    // Setup environment
    addFiles(sc,
      absPath("src/main/resources/Chicago_Ohare_International_Airport.csv"),
      absPath("src/main/resources/allyears2k_headers.csv.gz"))

    //val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = sc.textFile(enforceLocalSparkFile("Chicago_Ohare_International_Airport.csv"), 3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val airlinesData = new H2OFrame(new File(SparkFiles.get("allyears2k_headers.csv.gz")))

    val airlinesTable = h2oContext.asDataFrame(airlinesData)(sqlContext).map(row => AirlinesParse(row))
    // Select flights only to ORD
    val flightsToORD = airlinesTable.filter(f => f.Dest == Some("ORD"))

    flightsToORD.count
    println(s"\nFlights to ORD: ${flightsToORD.count}\n")

    flightsToORD.toDF.createOrReplaceTempView("FlightsToORD")
    weatherTable.toDF.createOrReplaceTempView("WeatherORD")
    //
    // -- Join both tables and select interesting columns
    //
    val bigTable = spark.sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
        |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day
        |WHERE f.ArrDelay IS NOT NULL""".stripMargin)

    val train: H2OFrame = bigTable.repartition(1) // This is trick to handle PUBDEV-928 - DeepLearning is failing on empty chunks

    //
    // -- Run DeepLearning
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._response_column = 'ArrDelay
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100, 100)

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    val predictionH2OFrame = dlModel.score(bigTable)('predict)
    val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    println(
      s"""# R script for residual plot
         |library(h2o)
         |h = h2o.init()
         |
        |pred = h2o.getFrame(h, "${predictionH2OFrame._key}")
         |act = h2o.getFrame (h, "${bigTable._key}")
         |
        |predDelay = pred$$predict
         |actDelay = act$$ArrDelay
         |
        |nrow(actDelay) == nrow(predDelay)
         |
        |residuals = predDelay - actDelay
         |
        |compare = cbind (as.data.frame(actDelay$$ArrDelay), as.data.frame(residuals$$predict))
         |nrow(compare)
         |plot( compare[,1:2] )
         |
      """.stripMargin)
    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)

  }

  }