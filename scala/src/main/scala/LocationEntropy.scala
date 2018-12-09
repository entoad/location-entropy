package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object LocationEntropy {
  def calculate(df: DataFrame) = {
    val totalVisits = df.select(sum("num_visits")).first().get(0)

    val singleLocationEntropyUDF = udf(singleLocationEntropy _)
    val negEntropy = df.groupBy("location").agg(sum("num_visits").alias("num_visits"))
        .withColumn("total_num_visits", lit(totalVisits).cast(IntegerType))
        .withColumn("entropy", singleLocationEntropyUDF(col("num_visits"), col("total_num_visits")))
        .select(sum("entropy"))
        .first().get(0).asInstanceOf[Double]
    -negEntropy
  }

  def singleLocationEntropy(visits: Int, totalVisits: Int): Double = {
    val p: Double = 1.0 * visits / totalVisits
    p * math.log10(p) / math.log10(2)
  }
}
