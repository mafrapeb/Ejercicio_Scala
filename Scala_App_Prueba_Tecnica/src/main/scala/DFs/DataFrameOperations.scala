package DFs

import org.apache.spark.sql.DataFrame

class DataFrameOperations {

  // Left join entre dos DataFrames
  def leftJoin(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, Seq("id"), "left")
  }

  // Inner join entre dos DataFrames
  def innerJoin(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, Seq("id"))
  }

  // Método para mostrar el resultado
  def showDataFrame(df: DataFrame, numRows: Int = 20): Unit = {
    df.show(numRows)
  }
}

