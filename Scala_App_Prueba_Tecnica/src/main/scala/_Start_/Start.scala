package _Start_

import DFs.DataFrameFactory
import DFs.OptimizedProcess
import DFs.DataFrameOperations
import org.apache.spark.sql.SparkSession

object Start {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Optimized Spark App")
      .master("local[*]")
      .getOrCreate()

    // Crear instancias de nuestras clases
    val dfFactory = new DataFrameFactory(spark)
    val dfOperations = new DataFrameOperations
    val optimizedProcess = new OptimizedProcess(spark)

    // Crear los DataFrames
    val df1 = dfFactory.createDF1()
    val df2 = dfFactory.createDF2()
    val df3 = dfFactory.createDF3()

    // Realizar el proceso normal
    val df4 = dfOperations.leftJoin(df1, df2)
    val df5 = dfOperations.innerJoin(df4, df3)
    dfOperations.showDataFrame(df5)

    // Realizar el proceso optimizado
    val df5Optimized = optimizedProcess.optimize(df1, df2, df3)
    optimizedProcess.showOptimizedResult(df5Optimized)

    // Realizar el proceso optimizado con reparticionamiento
    val df5Repartitioned = optimizedProcess.optimizeWithRepartition(df1, df2, df3)
    optimizedProcess.showOptimizedResult(df5Repartitioned)

    // Finalizar la sesión de Spark
    spark.stop()
  }
}
