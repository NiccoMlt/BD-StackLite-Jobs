package it.unibo.bd1819.common

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object FileParsing {

  val FIELD_SEPARATOR = ","

  /**
   * Map a string to a StructType schema
   *
   * @param schemaString   the string containing the schema
   * @param fieldSeparator the file separator used inside the schema string
   * @param filterCriteria a filter criteria for the fields, if necessary
   * @return a StructType containing the schema string
   */
  def StringToSchema(schemaString: String, fieldSeparator: String = FIELD_SEPARATOR,
                     filterCriteria: String => Boolean = _ => true): StructType =
    StructType(schemaString.split(fieldSeparator)
      .filter(filterCriteria)
      .map(fieldName => StructField(fieldName, StringType, nullable = true)))

}
