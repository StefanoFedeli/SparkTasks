package scala;

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io._;


class Trip(pickup: LocalDate, dropoff: LocalDate, distance: Float, sourceID: Int, destID: Int, tot: Float) {

  val pickup_datetime: LocalDate = pickup
  val dropoff_datetime: LocalDate = dropoff
  val trip_distance: Float = distance
  val puID: Int = sourceID
  val doID: Int = destID
  val total: Float = tot

}

object ETL {
  def main(args: Array[String]) {

    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("PreProcessData").setMaster("local[1]").set("spark.executor.memory","1g"))
    sc.setLogLevel("ALL")
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
    val tupleSet: DataFrame = cleaned_df.withColumn("key", concat(lit("("),col("PULocationID"),lit(","),col("DOLocationID"),lit(")")) ).groupBy("key").count().sort(col("count").desc)//.take(10)
    tupleSet.show(10)


    /*
    val pw = new PrintWriter(new File("/opt/spark-data/output/task2.txt"))
    //pw.write(cropped_df.show(3).toString())
    pw.write(cropped_df.count().toString())
    pw.write("\n")
    pw.write(q1.toString())
    pw.write("\n")
    pw.write(iqr.toString())
    pw.write("\n")
    pw.write(tupleSet.count().toString())
    pw.close
    */
    

    //tupleSet.write.format("csv").save("file:///opt/spark-data/output/tuple")
    //rawData.coalesce(1).write.option("header", "true").csv("file:///opt/spark-data/output/copped")

    /*
    import session.implicits._
    var cropped_df: Dataset[Trip] = rawData.map( (row) => new Trip(
          LocalDate.parse(row.getString(1),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
          LocalDate.parse(row.getString(2),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
          row.getFloat(4),
          row.getInt(7),
          row.getInt(8),
          row.getFloat(16)
        )
    )
    cropped_df.show()
    */
    sc.stop()
  }
}