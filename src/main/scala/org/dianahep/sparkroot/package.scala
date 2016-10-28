package org.dianahep

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader

package object sparkroot {
  implicit class RootDataFrameReader(reader: DataFrameReader) {
    def root: () => DataFrame =
      reader.format("org.dianahep.sparkroot").load
  }
}
