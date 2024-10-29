package de.datacleaner.spark
package models

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

abstract class baseClass {
  protected val df: DataFrame
  protected val _schema: Map[String, Map[String, String]]

  def validateColumns(): Unit = {
    val columns = df.columns.toList
    val diff = columns.diff(_schema.keys.toList)

    if (diff.length > 0)
      throw new Exception(s"The $diff are not in the schema model")
    df.columns.map(column =>
      lookupColumn(column)
    )
    val dfSchema = schemaToMap(df.schema)

    if (dfSchema != _schema) {
      throw new Exception(s"There are inconsistencies in the schema")
    }
  }

  def lookupColumn(column: String): Unit = {
    if (!(_schema contains column))
      throw new Exception(s"The $column is not in the schema")
  }

  def schemaToMap(schema: StructType): Map[String, Map[String, String]] = {
    schema.fields.map { field =>
      field.name -> Map(
        "dtype" -> field.dataType.typeName,
        "allowNull" -> field.nullable.toString
      )
    }.toMap
  }

}
