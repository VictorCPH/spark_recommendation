package inputData

import org.apache.spark.sql.DataFrame
import spark.SparkEnvironment.spark

/**
  * Created by chen on 2017/4/9.
  */
object HolidayDataHolder {
  protected val holidayData = loadHolidayData()

  private def loadHolidayData(): DataFrame = {
    val holidayData = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost/shared_bike")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "holiday_data")
      .option("user", "root")
      .option("password", "chen0724")
      .load()
    holidayData
  }

  def getHolidayData(): DataFrame = holidayData

  def main(args: Array[String]): Unit = {
    holidayData.show()
    spark.stop()
  }
}
