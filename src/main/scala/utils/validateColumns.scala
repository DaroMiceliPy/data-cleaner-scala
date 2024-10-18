package de.datacleaner.spark
package utils

case object validateColumns {
  def validateColumns(columns: Array[String]): Unit = {
    columns.map(column => iterateColumn(column))
    println("Succesfully validate columns!!!")
  }
  def iterateColumn(column: String): Unit = {
    val value = column != "NrodeDoc" & column != "telefonos" & column != "typePhone_v2"

    if (value)
      throw new Exception(s"The $column is necessary for pivoting")

  }
}
