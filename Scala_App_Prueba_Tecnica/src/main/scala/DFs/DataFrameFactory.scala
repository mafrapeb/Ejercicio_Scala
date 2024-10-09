package DFs

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameFactory(spark: SparkSession) {

  import spark.implicits._

  // Método para crear DataFrame df1 (4000 filas)
  def createDF1(): DataFrame = {
    (1 to 4000).map(i => (i, s"df1_val_$i")).toDF("id", "value1")
  }

  // Método para crear DataFrame df2 (1000 filas)
  def createDF2(): DataFrame = {
    (1 to 1000).map(i => (i, s"df2_val_$i")).toDF("id", "value2")
  }

  // Método para crear DataFrame df3 (10 filas)
  def createDF3(): DataFrame = {
    (1 to 10).map(i => (i, s"df3_val_$i")).toDF("id", "value3")
  }
}
