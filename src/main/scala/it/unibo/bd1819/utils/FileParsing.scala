package it.unibo.bd1819.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object FileParsing {

  val FIELD_SEPARATOR = ";"
  /**
    * Map a string to a StructType schema
    * @param schemaString the string containing the schema
    * @param fieldSeparator the file separator used inside the schema string
    * @param filterCriteria a filter criteria for the fields, if necessary
    * @return a StructType containing the schemastring
    */
  def StringToSchema(schemaString: String, fieldSeparator: String = FIELD_SEPARATOR,
                     filterCriteria:String => Boolean = _ => true) =
    StructType(schemaString.split(fieldSeparator)
        .filter(filterCriteria)
      .map(fieldName => StructField( fieldName, StringType, true)))

}
