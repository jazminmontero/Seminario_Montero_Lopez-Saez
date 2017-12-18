package es.arjon

import org.apache.spark.sql.SparkSession

/*
object RunAll {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: RunAll <dataset folder> <lookup file> <output folder>
           |  <dataset folder> folder where stocks data is located
           |  <lookup file> file containing lookup information
           |  <output folder> folder to write parquet data
           |
           |RunAll /dataset/stocks-small /dataset/yahoo-symbols-201709.csv /dataset/output.parquet
        """.stripMargin)
      System.exit(1)
    }

    val Array(stocksFolder, lookupSymbol, outputFolder) = args


    val spark = SparkSession.
      builder.
      appName("Stocks:ETL").
      getOrCreate()

    val stocksDS = ReadStockCSV.process(spark, stocksFolder)
    val lookup = ReadSymbolLookup.process(spark, lookupSymbol)

    // For implicit conversions like converting RDDs to DataFrames
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val ds = stocksDS.
      withColumn("full_date", unix_timestamp($"dateTime", "yyyy-MM-dd").cast("timestamp")).
      filter("full_date >= \"2017-09-01\"").
      withColumn("year", year($"full_date")).
      withColumn("month", month($"full_date")).
      withColumn("day", dayofmonth($"full_date")).
      drop($"dateTime").
      withColumnRenamed("name", "symbol").
      join(lookup, Seq("symbol"))

    // https://weishungchung.com/2016/08/21/spark-analyzing-stock-price/
    val movingAverageWindow20 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-20, 0)
    val movingAverageWindow50 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-50, 0)
    val movingAverageWindow100 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-100, 0)

    // Calculate the moving average
    val stocksMA = ds.
      withColumn("ma20", avg($"close").over(movingAverageWindow20)).
      withColumn("ma50", avg($"close").over(movingAverageWindow50)).
      withColumn("ma100", avg($"close").over(movingAverageWindow100))

    stocksMA.show(100)

    DatasetToParquet.process(spark, stocksMA, outputFolder)

    DatasetToPostgres.process(spark, stocksMA)

    spark.stop()
  }
}
*/
object RunAll {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: RunAll <dataset folder> <lookup file> <output folder>
           |  <dataset folder> folder where flights data is located
           |  <lookup file> file containing lookup information
           |  <output folder> folder to write parquet data
           |
           |RunAll /dataset/Flight /flight.txt /dataset/output.parquet

		""".stripMargin)
      System.exit(1)
    }

    // val Array(stocksFolder, lookupSymbol, outputFolder) = args
	val Array(flightFolder, lookupflight, outputFolder) = args

    val spark = SparkSession.
      builder.
      // appName("Stocks:ETL").
      appName("Flight:ETL").
	  getOrCreate()

    //val stocksDS = ReadStockCSV.process(spark, stocksFolder)
    //val lookup = ReadSymbolLookup.process(spark, lookupSymbol)
	val lookup = ReadAerolineaLookup.process(spark, lookupflight)
    import org.apache.spark.sql.functions._

    // For implicit conversions like converting RDDs to DataFrames
    val Flight = ReadFlightsTXT.process(spark, flightFolder)
    import spark.implicits._

    /* val ds = stocksDS.
      withColumn("full_date", unix_timestamp($"dateTime", "yyyy-MM-dd").cast("timestamp")).
      filter("full_date >= \"2017-09-01\"").
      withColumn("year", year($"full_date")).
      withColumn("month", month($"full_date")).
      withColumn("day", dayofmonth($"full_date")).
      drop($"dateTime").
      withColumnRenamed("name", "symbol").
      join(lookup, Seq("symbol"))
	*/

	val ds = Flight
      .withColumn("FechaHora", unix_timestamp($"dateTime", "dd-MM-yyyy HH:mm").cast("timestamp"))
      .filter("FechaHora >= \"2014-12-31 00:00\"")
      .withColumn("Year", year($"full_date"))
      .withColumn("Month", month($"full_date"))
      .withColumn("Day", dayofmonth($"full_date"))
      .drop($"dateTime")
      .withColumnRenamed("Aerolinea_Nombre", "flight")
    //.join(lookup, Seq("flight"))


	// NO TENEMOS QUE HACER NINGUN CALCULO ADICIONAL SOBRE LOS DATOS
	// *************************************************************
	// entonces lo que llama stocksMA, que es el dataset original con las columnas de los MA agregadas, no va... queda el flightsDS original

	// https://weishungchung.com/2016/08/21/spark-analyzing-stock-price/
    // val movingAverageWindow20 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-20, 0)
    // val movingAverageWindow50 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-50, 0)
    // val movingAverageWindow100 = Window.partitionBy($"symbol").orderBy("full_date").rowsBetween(-100, 0)

    // Calculate the moving average
    //val stocksMA = ds.
    //  withColumn("ma20", avg($"close").over(movingAverageWindow20)).
    //  withColumn("ma50", avg($"close").over(movingAverageWindow50)).
    //  withColumn("ma100", avg($"close").over(movingAverageWindow100))

   // ds.show(100)

    DatasetToParquet.process(spark, ds, destinationFolder = outputFolder)

    DatasetToPostgres.process(spark, ds)

    spark.stop()
  }
}
