package DFs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

class OptimizedProcess(spark: SparkSession) {

  // Método para aplicar un proceso optimizado, utilizando Broadcast
  def optimize(df1: DataFrame, df2: DataFrame, df3: DataFrame): DataFrame = {
    val dfOperations = new DataFrameOperations

    // Left join entre df1 y df2
    val df4 = dfOperations.leftJoin(df1, df2).cache()  // Cacheando df4 por si es reutilizado

    // Join optimizado con broadcast
    val df5 = dfOperations.innerJoin(df4, broadcast(df3))
    df5.cache()  // Cachear el resultado optimizado

    df5
  }

  // Método para aplicar reparticionamiento antes del proceso
  def optimizeWithRepartition(df1: DataFrame, df2: DataFrame, df3: DataFrame): DataFrame = {
    val dfOperations = new DataFrameOperations

    // Reparticionamos los DataFrames según el tamaño
    val df1Repartitioned = df1.repartition(10, df1("id"))
    val df2Repartitioned = df2.repartition(10, df2("id"))
    val df3Repartitioned = df3.repartition(1, df3("id"))  // df3 es pequeño, una partición es suficiente

    // Aplicar joins como antes
    val df4 = dfOperations.leftJoin(df1Repartitioned, df2Repartitioned).cache()
    val df5 = dfOperations.innerJoin(df4, broadcast(df3Repartitioned))
    df5.cache()

    df5
  }

  // Método para mostrar el resultado
  def showOptimizedResult(df: DataFrame, numRows: Int = 10): Unit = {
    df.show(numRows)
  }
}

