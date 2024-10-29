package de.datacleaner.spark
package models

import org.apache.spark.sql.DataFrame

class baseClientes(val df: DataFrame) extends baseClass {
  val _schema = Map(
    "NrodeDoc" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 1" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 2" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 3" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 4" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 5" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 6" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 7" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 8" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 9" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 10" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 11" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 12" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 13" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 14" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    ),
    "Telefono 15" -> Map(
      "dtype" -> "string",
      "allowNull" -> "true"
    )
  )
}
