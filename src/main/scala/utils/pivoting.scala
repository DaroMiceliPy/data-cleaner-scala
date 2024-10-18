package de.datacleaner.spark
package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, when}

case object pivoting {

  def pivotingDF(df: DataFrame): DataFrame = {
    validateColumns.validateColumns(df.columns)
    
    val pivoting: DataFrame = df
      .withColumn("Telefono_1", when(col("typePhone_v2") === "Telefono 1", col("telefonos")))
      .withColumn("Telefono_2", when(col("typePhone_v2") === "Telefono 2", col("telefonos")))
      .withColumn("Telefono_3", when(col("typePhone_v2") === "Telefono 3", col("telefonos")))
      .withColumn("Telefono_4", when(col("typePhone_v2") === "Telefono 4", col("telefonos")))
      .withColumn("Telefono_5", when(col("typePhone_v2") === "Telefono 5", col("telefonos")))
      .withColumn("Telefono_6", when(col("typePhone_v2") === "Telefono 6", col("telefonos")))
      .withColumn("Telefono_7", when(col("typePhone_v2") === "Telefono 7", col("telefonos")))
      .withColumn("Telefono_8", when(col("typePhone_v2") === "Telefono 8", col("telefonos")))
      .withColumn("Telefono_9", when(col("typePhone_v2") === "Telefono 9", col("telefonos")))
      .withColumn("Telefono_10", when(col("typePhone_v2") === "Telefono 10", col("telefonos")))
      .withColumn("Telefono_11", when(col("typePhone_v2") === "Telefono 11", col("telefonos")))
      .withColumn("Telefono_12", when(col("typePhone_v2") === "Telefono 12", col("telefonos")))
      .withColumn("Telefono_13", when(col("typePhone_v2") === "Telefono 13", col("telefonos")))
      .withColumn("Telefono_14", when(col("typePhone_v2") === "Telefono 14", col("telefonos")))
      .withColumn("Telefono_15", when(col("typePhone_v2") === "Telefono 15", col("telefonos")))
      .groupBy("NrodeDoc")
      .agg(
        max(col("Telefono_1")).as("Telefono 1"),
        max(col("Telefono_2")).as("Telefono 2"),
        max(col("Telefono_3")).as("Telefono 3"),
        max(col("Telefono_4")).as("Telefono 4"),
        max(col("Telefono_5")).as("Telefono 5"),
        max(col("Telefono_6")).as("Telefono 6"),
        max(col("Telefono_7")).as("Telefono 7"),
        max(col("Telefono_8")).as("Telefono 8"),
        max(col("Telefono_9")).as("Telefono 9"),
        max(col("Telefono_10")).as("Telefono 10"),
        max(col("Telefono_11")).as("Telefono 11"),
        max(col("Telefono_12")).as("Telefono 12"),
        max(col("Telefono_13")).as("Telefono 13"),
        max(col("Telefono_14")).as("Telefono 14"),
        max(col("Telefono_15")).as("Telefono 15"),
      )
      .select("NrodeDoc",
        "Telefono 1", "Telefono 2",
        "Telefono 3", "Telefono 4",
        "Telefono 5", "Telefono 6",
        "Telefono 7", "Telefono 8",
        "Telefono 9", "Telefono 10",
        "Telefono 11", "Telefono 12",
        "Telefono 13", "Telefono 14",
        "Telefono 15"
      )

    pivoting
  }
}


