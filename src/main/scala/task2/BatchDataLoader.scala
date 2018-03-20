package task2

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import shared.{DataModel, LoaderUtils}

import scala.util.{Success, Try}



object BatchDataLoader {

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

    log.info("Calculating list of weeks")
    val weeks = LoaderUtils.time{flightData.toDF().select("observation_week").distinct.map(r => r(0).asInstanceOf[Int]).
      collect().toList.sorted}

    log.info("Weeks length: " + weeks.length)

    /*
    for (week <- weeks) {
      val filteredFlightData = flightData.filter(record => record.observation_week == week)
      filteredFlightData.saveToEs("flight-{observation_week}/rows")
    }
    */
    //for (week <- weeks) {
      val week = weeks(100)
      val filteredFlightData = flightData.filter(record => record.observation_week == week)

      val convertedFlightData = filteredFlightData.mapPartitions(
        iter => {

          // TODO here should be HTTPClient initialization - not working

          val updatedIter = iter.map(
            item => {
                Try(LoaderUtils.getConvertedCurrency(item.trip_price_avg))
                .transform(
                      { b => Success(Right(item.copy(trip_price_avg_2 = b))) },
                      { e => Success(Left(item.copy(trip_price_avg_2 = -1.0))) }
                  ).get
              //item.copy(trip_price_avg_2 = convertedPrice)
            }
          )

          // TODO here should be HTTPClient shutdown

          updatedIter
        })

      convertedFlightData
    .filter(el => el.isRight)
      .map(el => el.right.get)
      .saveToEs("flight-{observation_week}/rows")
    convertedFlightData
      .filter(el => el.isLeft)
      .map(el => el.left.get)
      .saveToEs("flight-{observation_week}/rows")


    //}
    // filteredFlightData.saveToEs("flight-{observation_week}/rows")

    System.in.read
    spark.stop
  }

}