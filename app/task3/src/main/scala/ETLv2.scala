package scala;

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io._;


/**
 * ETL object. Contains the logic for the third task
 */
object ETLv2 {

    /**
  * Map single rows into a list of strings
  * @param source Pickup ID
  * @param dest DropOff ID
  */
  def mapToList(source: Int, dest: Int): List[String] = {
    var bunch: List[String] = List()
    for (pick <- source-1 to source+1) {
      for (out <- dest-1 to dest+1) {
        val str_new = "(" + pick.toString() + " " + out.toString() + ")"
        str_new :: bunch 
      }
    }
    return bunch
  }

  def main(args: Array[String]) {

    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("PreProcessDataExpanded").setMaster("local[1]").set("spark.executor.memory","1g"))
    //sc.setLogLevel("ALL")
    val session: SparkSession = SparkSession.builder().getOrCreate()
    import session.implicits._

    // OUTLIERS DETECTION WITH IQR METHOD
    val rawData: DataFrame = session.read.option("header","true").option("inferSchema", "true").csv("file:///opt/spark-data/raw/yellow.csv")
    val cropped_df: DataFrame = rawData.drop("VendorID","passenger_count","RatecodeID","store_and_fwd_flag","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge")
    val quantiles = cropped_df.stat.approxQuantile("trip_distance",Array(0.25,0.75),0.0)
    val q1: Double = quantiles(0)
    val q3: Double = quantiles(1)
    val iqr: Double = q3 - q1

    val lowerRange: Double = q1 - 1.5*iqr
    val upperRange: Double = q3 + 1.5*iqr

    val cleaned_df: DataFrame = cropped_df.filter(s"trip_distance > $lowerRange and trip_distance < $upperRange")


    // ACTUAL TASK
    val expadendLocations: Dataset[String] = cleaned_df.flatMap(row =>  mapToList(row.getInt(3),row.getInt(4)) ).groupBy("key").count().sort(col("count").desc)
  
    expadendLocations.show(10)

    sc.stop()
  }
}