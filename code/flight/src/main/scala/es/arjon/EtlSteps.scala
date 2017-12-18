package es.arjon

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/*CREATE SCHEMA workshop;*/

/*case class Stock(name: String,
                 dateTime: String,
                 open: Double,
                 high: Double,
                 low: Double,
                 close: Double)
*/



/*
object Stock {
  def fromCSV(symbol: String, line: String): Option[Stock] = {
    val v = line.split(",")

    try {
      Some(
        Stock(
          symbol,
          dateTime = v(0),
          open = v(1).toDouble,
          high = v(2).toDouble,
          low = v(3).toDouble,
          close = v(4).toDouble
        )
      )

    } catch {
      case ex: Exception => {
        println(s"Failed to process $symbol, with input $line, with ${ex.toString}")
        None
      }
    }

  }
}
*/




/*
object ReadStockCSV {
  def process(spark: SparkSession, originFolder: String) = {

    // Using SparkContext to use RDD
    val sc = spark.sparkContext
    val files = sc.wholeTextFiles(originFolder, minPartitions = 40)

    val stocks = files.map { case (filename, content) =>
      val symbol = new java.io.File(filename).
        getName.
        split('.')(0).
        toUpperCase

      content.split("\n").flatMap { line =>
        Stock.fromCSV(symbol, line)
      }
    }.
      flatMap(e => e).
      cache

    import spark.implicits._

    stocks.toDS.as[Stock]
  }
}
*/
object ReadFlightsTXT {
  def process(spark: SparkSession, originFolder: String): Dataset[Flight] = {

    // Using SparkContext to use RDD
    val sc = spark.sparkContext
    val files = sc.wholeTextFiles(originFolder, minPartitions = 40)

    val flights = files.map { case (filename, content) =>
      val flight = new java.io.File(filename).
        getName.
        split('.')(0).
        toUpperCase

      content.split("\n").flatMap { line =>
        Flight.fromTXT(flight, line)
      }
    }.
      flatMap(e => e).
      cache

    import spark.implicits._

    flights.toDS.as[Flight]
  }
}


/*object ReadSymbolLookup {
  def process(spark: SparkSession, file: String) = {
    import spark.implicits._
    spark.read.
      option("header", true).
      option("inferSchema", true).
      csv(file).
      select($"Ticker", $"Category Name").
      withColumnRenamed("Ticker", "symbol").
      withColumnRenamed("Category Name", "category")
  }
}
*/
object ReadAerolineaLookup {
  def process(spark: SparkSession, file: String): Any = {
    import spark.implicits._
    spark.read.
      option("header", true).
      option("inferSchema", true).
      csv(file).
      select($"Aerolinea_OACI", $"Aerolinea").
      withColumnRenamed("Aerolinea_OACI", "Aerolinea_Cod").
      withColumnRenamed("Aerolinea", "Aerolinea_Nombre")
  }
}

object DatasetToParquet {
  def process(spark: SparkSession, df: DataFrame, destinationFolder: String): Unit = {
    // https://stackoverflow.com/questions/43731679/how-to-save-a-partitioned-parquet-file-in-spark-2-1
    df.
      write.
      mode("overwrite").
      partitionBy("year", "month", "day").
      parquet(destinationFolder)
  }
}

object DatasetToPostgres {

  def process(spark: SparkSession, df: DataFrame): Unit = {
    // Write to Postgres
    val connectionProperties = new java.util.Properties
    connectionProperties.put("user", "workshop")
    connectionProperties.put("password", "w0rkzh0p")
    val jdbcUrl = s"jdbc:postgresql://postgres:5432/workshop"

    df.
      drop("year", "month", "day"). // drop unused columns
      write.
      mode(SaveMode.Append).
      //jdbc(jdbcUrl, "stocks", connectionProperties)
	  jdbc(jdbcUrl, "Flight", connectionProperties)
  }
}

// TODO: Read compressed
// option("codec", "org.apache.hadoop.io.compress.GzipCodec").
