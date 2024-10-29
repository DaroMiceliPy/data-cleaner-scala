package de.datacleaner.spark
package models

import org.apache.spark.sql.DataFrame

class agregarBase(val df: DataFrame) extends baseClass {
  val _schema = Map(
    "NrodeDoc" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefonos" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    )
  )

}
