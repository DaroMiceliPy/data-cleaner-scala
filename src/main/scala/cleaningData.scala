package de.datacleaner.spark

import de.datacleaner.spark.models.telefonosErroneos
import de.datacleaner.spark.utils.pivoting.pivotingDF
import de.datacleaner.spark.models.baseClientes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, isnan, isnull, lit, max, row_number, when}

object cleaningData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("cleaning-data")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()


    val baseClientesDf: DataFrame = spark.read
      .option("header", value = true)
      .option("delimiter", value = ";")
      .csv("data/Base_clientes.csv")

    baseClientesDf.printSchema();

    val validateBaseClientes = new baseClientes(baseClientesDf)
    validateBaseClientes.validateColumns()

    val telefonosErroneosDf: DataFrame = spark.read
      .option("header", value = true)
      .option("delimiter", value = ";")
      .csv("data/Telefonos_erroneos.csv")

    val validateTelefonosErroneos = new telefonosErroneos(telefonosErroneosDf)
    validateTelefonosErroneos.validateColumns()

    val telefonosErroneos_v2 = telefonosErroneosDf.select(
      col("Telefonos").as("telefonos"),
      lit("_both").as("_merge"))

    val baseMelt: DataFrame =
      baseClientesDf.melt(ids = Array(col("NrodeDoc")),
        variableColumnName = "typePhone", 
        valueColumnName = "telefonos")

    val merge = baseMelt.join(telefonosErroneos_v2,
      usingColumn = "telefonos", joinType = "left")
      .filter(isnull(col("_merge")))
      .select("NrodeDoc", "typePhone", "telefonos")
    

    val dnisDosentExists = baseClientesDf
      .filter(!(col("NrodeDoc").isin(merge.col("NrodeDoc"))))
      .withColumn("typePhone", lit("NULL"))
      .withColumn("telefonos", lit("NULL"))
      .select("NrodeDoc", "typePhone", "telefonos")

    val beforePivoting = merge.union(dnisDosentExists)
      .withColumn("rank", row_number().over(Window.partitionBy("NrodeDoc").orderBy(col("telefonos").desc)))
      .withColumn("typePhone_v2", concat(lit("Telefono "), col("rank")))
      .select("NrodeDoc", "telefonos", "typePhone_v2")

    val finalDf = pivotingDF(beforePivoting)
    finalDf.show()

  
  }
}