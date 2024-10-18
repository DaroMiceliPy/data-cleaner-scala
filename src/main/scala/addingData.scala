package de.datacleaner.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, lit, max, row_number, when}
import utils.pivoting.pivotingDF

case object addingData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("addin-data")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val addingData = spark.read
      .option("header", value = true)
      .option("delimiter", value = ";")
      .csv("data/Agregar_base.csv")
      .select(col("NrodeDoc"), col("Telefonos").as("telefonos"))

    val baseClientes = spark.read
      .option("header", value = true)
      .option("delimiter", value = ";")
      .csv("data/Base_clientes.csv")

    val baseMelt = baseClientes.melt(Array(col("NrodeDoc")),
      variableColumnName = "typePhone_v2",
      valueColumnName = "telefonos")
      .select("NrodeDoc", "telefonos")

    val concatDf = baseMelt.union(addingData)
      .withColumn("rank", row_number().over(Window.partitionBy("NrodeDoc").orderBy(col("telefonos").desc)))
      .withColumn("typePhone_v2", concat(lit("Telefono "), col("rank")))
      .select("NrodeDoc", "telefonos", "typePhone_v2")



    val finalDf = pivotingDF(concatDf)
    finalDf.show()



  }
}
