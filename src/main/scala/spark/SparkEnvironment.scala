package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by chen on 2017/3/29.
  */
object SparkEnvironment {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  // set up environment
  val spark = SparkSession
    .builder()
    .appName(this.getClass.getName)
    .getOrCreate()
}
