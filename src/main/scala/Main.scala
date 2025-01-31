package com.aciesidle.training

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, row_number, year}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/** @note
  *   for this to run for java 17 we will need Vm option in configuration
  *   --add-exports java.base/sun.nio.ch=ALL-UNNAMED
  */

object Main {
  def main(args: Array[String]): Unit = {

    println("imported implicits")
    val spark = SparkSession
      .builder()
      .appName("file-read-session")
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AMPL.csv")

    /*  Data Frame col Referencing

    // val ds = df.as("as")
    // df.select("Date", "Open", "Close").show() // either to pass column name
    // other way
    // df.select(df("Date"), $"Open", col("Close")).show() // or to pass Column
    // df.select("Date", $"Open", col("Close")).show()  this will give compiler issue

    val column = df.apply("Open")
    column.plus(1.0)
    val newColumn = (column + 1.0).as("Open + By 1.0") // for column name
    val stringColumn = column.cast(StringType)

    val litColumn = lit(2.0) // give column based on any given literal

    val newStringCol = concat(stringColumn, lit(" Help")).as("stringify")

    df.select(column, newColumn, stringColumn, newStringCol, litColumn)
      .filter(newColumn > 1.6)
      .show(30, truncate = false)
     */

    /*    SQL expression in spark

  // here value either must be an expression or sqlText
    val timStCol1 = expr(
      "cast(current_timestamp() as string) as timeStampExpressionString"
    )
    val timStCol2 =
      current_timestamp().cast(StringType).as("time_stamp_function")
    df.select(timStCol1, timStCol2).show(truncate = false)

    df.selectExpr(
      "cast(Date as string)",
      "Open + 1.0",
      "current_timestamp()"
    ).show(truncate = false) // run Sql queries;

    df.createTempView("df") // need to register df as table for below sql queries to run
    spark.sql("select * from df").show()
     */

    /** @note
      *   Assignment
      *   - Rename all column to be a camel case format.
      *   - Add a column containing the diff between 'open' and 'close'.
      *   - Filter to days when the 'close' price was more than 10% higher than
      *     the open price.
      */

    //    df.withColumnRenamed("Open", "open")
    //      .withColumnRenamed("Close", "close")
    val renamedColumn = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    val stockData = df.select(renamedColumn: _*)
    stockData.createTempView("stockData")

    import spark.implicits._
    stockData.select($"date")

    // similar to rest operator we have => var args splice =>  "[]:_*"

//    val stockData = df.select(renamedColumn: _*)

//    val listOfColumns = df.columns.map(c => col(c).as(c.toLowerCase()))
//    df.select(listOfColumns: _*).show()

    //    stockData
    //      .withColumn("diffBetOpenAndClose", col("open") - col("close"))
    //      .filter(col("close") > col("open") * 1.10)
    //      .show(20, truncate = false)

    //    spark.sql("select * from stockData where close > open * 1.10 ")
    //      .show(20)

    /** @note
      *   GroupBY , Sort & Aggregate
      *   - sort on one or multiple column
      *   - GroupBY on or multiple columns and applying an aggregation on groups
      *   - Average and highest closing prices per year , sorted , with the
      *     highest prices first
      */

    // scala version
    //    stockData
    //      .filter(col("close") > col("open") * 1.10)
    //      .groupBy(year($"date").as("year"))
    //      .agg(max($"close").as("max_price"), functions.avg($"close").as("average"))
    //      .sort($"year".asc)
    ////      .show()
    //
    //    // sql version
    //    spark
    //      .sql(
    //        "SELECT YEAR(date) AS year,MAX(close) AS max_close,AVG(close) AS avg_close FROM stockData where close > open *1.10 GROUP BY year Order by year ASC"
    //      )
    ////      .show()
    //
    //    stockData
    //      .groupBy($"date")
    //      .max("close", "high")
    //      .sort($"date".asc)
    //      .show()
    //
    //    spark
    //      .sql(
    //        "select date , MAX(close), MAX(high) from stockData Group By date Order By date ASC"
    //      )
    //      .show()

    /** @note
      *   window function
      *   - Find the rows of the highest closing price in each year , sorted
      *     with the highest closing price first
      */

    stockData
      .groupBy(year($"date").as("year"))
      .agg(
        functions.max($"close").as("maxClose"),
        functions.avg($"close").as("avgClose")
      )
      .sort($"maxClose".desc)
//      .show()

    val window =
      Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
//
//    stockData
//      .withColumn("rank", row_number().over(window))
//      .filter($"rank" === 1)
//      .sort($"close".desc)
//      .show()

//    spark
//      .sql(""" SELECT *
//         |  FROM (
//         |    SELECT e.*,
//         |           row_number() OVER (PARTITION BY YEAR(date) ORDER BY close DESC) AS rank
//         |    FROM stockData  e
//         |  ) AS ranked_data
//         |  WHERE rank = 1
//         |  ORDER BY close DESC""".stripMargin)
//      .show(30, truncate = false)

    /** @note Abstract computational plan */

    var highestClosingPrices = highestClosingPricesPerYear(stockData, window)

  }

  def highestClosingPricesPerYear(
      stockData: DataFrame,
      window: WindowSpec
  ): DataFrame = {
    import stockData.sparkSession.implicits._
    stockData
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"close".desc)
//      .explain(extended = true)
  }

  def add(x: Int, y: Int): Int = x + y

}
