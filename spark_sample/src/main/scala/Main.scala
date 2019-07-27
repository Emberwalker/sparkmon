import org.apache.spark.sql.SparkSession

import scala.util.Random

object Main {
    def main(args: Array[String]): Unit = {
        val session = SparkSession.builder().master("local").appName("SparkTestServer").getOrCreate()
        import session.implicits._
        session
          .createDataFrame(Range(0,1024).map(Tuple1.apply))
          .repartition(1024)
          .map(_ => {
              // Simulate running job
              val delay = 500 + Random.nextInt(2000)
              Thread.sleep(delay)
              delay
          })
          .collect()
        session.stop()
    }
}