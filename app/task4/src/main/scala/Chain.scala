package scala;

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io._;


/**
 * Chain object. Contains the logic for the four task
 */
object Chain {
  def main(args: Array[String]) {

    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("LongestChain").setMaster("local[1]").set("spark.executor.memory","1g"))
    val session: SparkSession = SparkSession.builder().getOrCreate()
    import session.implicits._

    val rawData: DataFrame = session.read.option("header","true").option("inferSchema", "true").csv("file:///opt/spark-data/raw/yellow_tripdata_2018-09.csv").sample(0.01)
    val cropped_df: DataFrame = rawData.drop("VendorID","passenger_count","RatecodeID","store_and_fwd_flag","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge")
    val quantiles = cropped_df.stat.approxQuantile("trip_distance",Array(0.25,0.75),0.0)
    val q1: Double = quantiles(0)
    val q3: Double = quantiles(1)
    val iqr: Double = q3 - q1

    val lowerRange: Double = q1 - 1.5*iqr
    val upperRange: Double = q3 + 1.5*iqr

    val cleaned_df: DataFrame = cropped_df.filter(s"trip_distance > $lowerRange and trip_distance < $upperRange")

    sc.stop()
  }
}