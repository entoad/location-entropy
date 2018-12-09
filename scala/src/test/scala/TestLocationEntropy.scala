package test.scala

import main.scala.LocationEntropy
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest._

class TestLocationEntropy extends FunSuite {
  val schema = StructType(List(StructField("location", StringType), StructField("num_visits", IntegerType)))
  val spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
  val sc = spark.sparkContext

  def seqToDf(list: Seq[Row]): DataFrame = {
     spark.createDataFrame(sc.parallelize(list), schema)
  }

  def assertDoubleEquals(list: Seq[Row], expected: Double) = {
    val df = seqToDf(list)
    assert(math.abs(expected - LocationEntropy.calculate(df)) <= 0.001)
  }

  test("test small dataset") {
    assertResult(0) {
      val list = Seq(Row("loc", 1))
      val df = seqToDf(list)
      LocationEntropy.calculate(df)
    }

    val list = Seq(Row("loc2", 1), Row("loc3", 1))
    LocationEntropy.calculate(seqToDf(list))
    assertDoubleEquals(list, math.abs(-2 * 0.5 * math.log10(0.5) / math.log10(2)))
  }
}
