package task2

import org.apache.log4j.{LogManager, Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import shared.{DataModel, LoaderUtils}

import scala.util.{Success, Try}


object BatchDataLoader {

  PropertyConfigurator.configure("src/main/resources/log4j.properties")
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark DataLoader application")
      .config("es.index.auto.create", "true")
      .config("es.resource", "flight_price/rows")
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

    // flightData.take(10).foreach(println)
    // flightData.toDF().printSchema

    log.info("Reading csv file.")
    var (timeDiff, rowCount) = LoaderUtils.time {
      flightData.count()
    }
    log.info("Got " + rowCount + " rows. Avg row/sec is " + rowCount / timeDiff)

    log.info("Number of original partitions: " + flightData.getNumPartitions.toString)
    log.info("Partitions size: " + flightData.partitions.length)

    log.info("Calculating list of weeks")
    val weeks = flightData.toDF().select("observation_week").distinct.map(r => r(0).asInstanceOf[Int]).
      collect().toList.sorted

    log.info("Weeks length: " + weeks.length)


    val iterNbr = 2
    for (week <- weeks.dropRight(15).takeRight(iterNbr)) {
      log.info("Processing week " + week)
      val filteredFlightData = flightData.filter(record => record.observation_week == week)
      rowCount = filteredFlightData.count()

      val (timeDiff2, _) = LoaderUtils.time {
         val convertedFlightData = filteredFlightData.mapPartitions(
          iter => {

            // TODO here should be HTTPClient initialization - not working

            val updatedIter = iter.map(
              item => {
                Try(LoaderUtils.getConvertedCurrency(item.trip_price_avg))
                  .transform(
                    { b =>
                      // log.info("Price: " + item.trip_price_avg + ". Converted: " + b)
                      Success(Right(item.copy(trip_price_avg_2 = b)))
                    },
                    { e =>
                      // log.info("Got exception while converting the price")
                      Success(Left(item.copy(trip_price_avg_2 = -1.0)))
                    } // just to mark failure
                  ).get
                //item.copy(trip_price_avg_2 = convertedPrice)
              }
            )

            // TODO here should be HTTPClient shutdown

            updatedIter
          })

        // different processing for 2 data streams - save it both for now
        convertedFlightData
          .filter(el => el.isRight)
          .map(el => el.right.get)
          .saveToEs("flight_price-{observation_week}/rows")
        convertedFlightData
          .filter(el => el.isLeft)
          .map(el => el.left.get)
          .saveToEs("flight_price-{observation_week}/rows")
      }
      log.info("Avg row/sec is " + rowCount / timeDiff2)
    }

    // filteredFlightData.saveToEs("flight-{observation_week}/rows")

    System.in.read
    spark.stop
  }

}