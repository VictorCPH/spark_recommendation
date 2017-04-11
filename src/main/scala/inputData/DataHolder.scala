package inputData

import org.apache.spark.sql.DataFrame
import spark.SparkEnvironment.spark

/**
  * Created by chen on 2017/4/10.
  */
object DataHolder {
  protected val finalData = generateFinalData()

  def generateFinalData(): DataFrame = {
    val citiBikeTargetData = BikeDataHolder.getCitiBikeTargetData()
    val weatherData = WeatherDataHolder.getWeatherData()
    val holidayData = HolidayDataHolder.getHolidayData()

    citiBikeTargetData.createOrReplaceTempView("citi_bike_target_data")
    weatherData.createOrReplaceTempView("weather_data")
    holidayData.createOrReplaceTempView("holiday_data")

    val finalData = spark.sql(
      """
        |select cnt, c.year, c.month, c.day, c.hour, subscriber_cnt,
        |       customer_cnt, birth_1930s_cnt, birth_1930s_cnt,
        |       birth_1940s_cnt, birth_1950s_cnt, birth_1960s_cnt,
        |       birth_1970s_cnt, birth_1980s_cnt, birth_1990s_cnt,
        |       birth_2000s_cnt, birth_other_cnt, unknown_cnt,
        |       male_cnt, female_cnt, temperature, humidity, windspeed,
        |       weatherType, holiday, weekday
        |from citi_bike_target_data as c, weather_data as w, holiday_data as h
        |where c.year = w.year and w.year = h.year
        |  and c.month = w.month and w.month = h.month
        |  and c.day = w.day and w.day = h.day
        |  and c.hour = w.hour
        |order by c.year, c.month, c.day, c.hour
      """.stripMargin)
    finalData
  }

  def getFinalData(): DataFrame = finalData

  def main(args: Array[String]): Unit = {
    finalData.show()
    spark.stop()
  }
}
