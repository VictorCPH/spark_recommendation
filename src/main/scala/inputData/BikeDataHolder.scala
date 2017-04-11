package inputData

import spark.SparkEnvironment.spark
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StringType, StructType}
import org.apache.spark.sql.DataFrame
/**
  * Created by chen on 2017/3/27.
  */
object BikeDataHolder {
  // "/Users/chen/Downloads/citibike/*.csv"
  val dataPath = "/Users/chen/Downloads/citibike/2013*-citibike-tripdata.csv"
  protected val citiBikeRawData = loadDataFromCSV(dataPath)
  protected val citiBikeTargetData = generateTargetData(citiBikeRawData)

  /**
    * 从CSV文件中读取citibike数据为DataFrame
    * @param dataPath: String
    * @return citiBikeRawData: DataFrame
    */
  private def loadDataFromCSV(dataPath: String): DataFrame = {
    /*
     format: (tripDuration, startTime, stopTime, startStationId, startStationName, startStationLatitude,
      startStationLongitude, endStationId, endStationName, endStationLatitude, endStationLongitude,
      bikeId, userType, birthYear, gender)
     */
    val dataSchema = StructType(Array(
        StructField("tripDuration", IntegerType, true),
        StructField("startTime", StringType, true),
        StructField("stopTime", StringType, true),
        StructField("startStationId", IntegerType, true),
        StructField("startStationName", StringType, true),
        StructField("startStationLatitude", DoubleType, true),
        StructField("startStationLongitude", DoubleType, true),
        StructField("endStationId", IntegerType, true),
        StructField("endStationName", StringType, true),
        StructField("endStationLatitude", DoubleType, true),
        StructField("endStationLongitude", DoubleType, true),
        StructField("bikeId", IntegerType, true),
        StructField("userType", StringType, true),
        StructField("birthYear", StringType, true),
        StructField("gender", StringType, true)))

    val df = spark.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .schema(dataSchema)
        .load(dataPath)
    df
  }

  /**
    *
    * @param citiBikeRawData: DataFrame
    * @return citiBikeTargetData: DataFrame
    */
  private def generateTargetData(citiBikeRawData: DataFrame): DataFrame = {
    citiBikeRawData.createOrReplaceTempView("citi_bike_raw")
    var citiBikeTargetData = spark.sql("""
        |select count(*) as cnt,
        |cast(substring(startTimeHour, 1, 4) as int) year,
        |cast(substring(startTimeHour, 6, 2) as int) month,
        |cast(substring(startTimeHour, 9, 2) as int) day,
        |cast(substring(startTimeHour, 12, 2) as int) hour,
        |sum(if(userType = 'Subscriber', 1, 0)) subscriber_cnt,
        |sum(if(userType = 'Customer', 1, 0)) customer_cnt,
        |sum(if(birthYear >= '1930' and birthYear < '1940', 1, 0)) birth_1930s_cnt,
        |sum(if(birthYear >= '1940' and birthYear < '1950', 1, 0)) birth_1940s_cnt,
        |sum(if(birthYear >= '1950' and birthYear < '1960', 1, 0)) birth_1950s_cnt,
        |sum(if(birthYear >= '1960' and birthYear < '1970', 1, 0)) birth_1960s_cnt,
        |sum(if(birthYear >= '1970' and birthYear < '1980', 1, 0)) birth_1970s_cnt,
        |sum(if(birthYear >= '1980' and birthYear < '1990', 1, 0)) birth_1980s_cnt,
        |sum(if(birthYear >= '1990' and birthYear < '2000', 1, 0)) birth_1990s_cnt,
        |sum(if(birthYear >= '2000' and birthYear < '2010', 1, 0)) birth_2000s_cnt,
        |sum(if(birthYear >= '2010' or birthYear < '1930', 1, 0)) birth_other_cnt,
        |sum(if(gender = 0, 1, 0)) unknown_cnt,
        |sum(if(gender = 1, 1, 0)) male_cnt,
        |sum(if(gender = 2, 1, 0)) female_cnt
        |from
        |(
        |  select substring(startTime, 1, 13) as startTimeHour, userType, birthYear, gender
        |  from citi_bike_raw
        |)
        |group by startTimeHour
        |order by startTimeHour
       """.stripMargin)
    citiBikeTargetData
  }

  def getCitiBikeRawData(): DataFrame = citiBikeRawData

  def getCitiBikeTargetData(): DataFrame = citiBikeTargetData

  def main(args: Array[String]): Unit = {
    citiBikeRawData.show
    citiBikeTargetData.show
    spark.stop()
  }
}

