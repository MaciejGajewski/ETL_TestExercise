package task1

import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.spark._
import org.apache.log4j.LogManager
import org.apache.log4j.PropertyConfigurator
import shared.{DataModel, LoaderUtils}


object SimpleDataLoader {

  PropertyConfigurator.configure("src/main/resources/log4j.properties")
  val log = LogManager.getRootLogger()

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark DataLoader application")
      .config("es.index.auto.create", "true")
      .config("es.resource", "flight/rows")
      .config("es.batch.size.bytes", "1mb")
      .config("es.batch.size.entries", 1000)
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    import spark.implicits._

    //spark.sparkContext.setLogLevel("ERROR")

    log.info("Starting Simple Data Loader")

    val flightData = spark.sparkContext.textFile("src/main/resources/part-00000.gz").
        map(s => new DataModel(s.split("\t", -1))).repartition(2) //.cache())

    //flightData.take(10).foreach(println)
    // flightData.toDF().printSchema

    log.info("Reading csv file.")
    val rowCount = LoaderUtils.time{flightData.count()}
    log.info("Got " + rowCount + " rows.")

    log.info("Number of original partitions: " + flightData.getNumPartitions.toString)
    log.info("Partitions size: " + flightData.partitions.size)

    log.info("Saving data to ElasticSearch to single index")
    //LoaderUtils.time{flightData.saveToEs("flight/rows")} // this is OK 15 min for part-000000.gz file but without buckets

    //flightData.saveToEs("flight-{observation_week}/rows") // this take ages, killed after 2 hrs for 2 part files

    log.info("Calculating list of weeks")
    val weeks = LoaderUtils.time{flightData.toDF().select("observation_week").distinct.map(r => r(0).asInstanceOf[Int]).
      collect().toList.sorted}

    log.info("Weeks length: " + weeks.length)

    log.info("Saving data to ElasticSearch partitioned by week")
    val iterNbr = 5
    for (week <- weeks.take(iterNbr)) {
      log.info("Week number " + week)
      log.info("Filtering")
      val filteredFlightData = flightData.filter(record => record.observation_week == week)
      val count = LoaderUtils.time{filteredFlightData.count()}
      log.info("Filtered count " + count)

      log.info("Saving")
      LoaderUtils.time {
        filteredFlightData.saveToEs("flight2-{observation_week}/rows")
      }
    }

    System.in.read
    spark.stop
  }
}