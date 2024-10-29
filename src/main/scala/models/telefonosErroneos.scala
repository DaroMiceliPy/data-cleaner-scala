package de.datacleaner.spark
package models

import org.apache.spark.sql.DataFrame


class telefonosErroneos(val df: DataFrame) extends baseClass {
   val _schema = Map(
    "Telefonos" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    )
  )
}


