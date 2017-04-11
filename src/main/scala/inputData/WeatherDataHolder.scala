package inputData

import spark.SparkEnvironment.spark
import org.apache.spark.sql.DataFrame

/**
  * Created by chen on 2017/4/9.
  */
object WeatherDataHolder {
  protected val weatherData = loadWeatherData()

  private def loadWeatherData(): DataFrame = {
    val weatherData = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost/shared_bike")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "weather_data")
      .option("user", "root")
      .option("password", "chen0724")
      .load()
    weatherData
  }

  def getWeatherData(): DataFrame = weatherData

  def main(args: Array[String]): Unit = {
    weatherData.show()
    spark.stop()
  }
}
