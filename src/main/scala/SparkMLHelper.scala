import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by XiangChen1 on 8/17/16.
 */
trait SparkMLHelper {
  protected def initializeSpark(): (SparkContext, SQLContext) = {
    System.setProperty("hadoop.home.dir", "I:\\spark-1.6.0-bin-hadoop2.6")
    val conf = new SparkConf()
    conf.set("spark.app.name", "Spark Core Test")
    conf.set("spark.master", "local[*]")
    conf.set("spark.ui.port", "36000")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    (sc, sqlContext)
  }

  protected def loadCSVData(sqlContext:SQLContext, fileName:String): DataFrame = {
    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("parserLib", "univocity")
      .load(fileName)

    df
  }
}
