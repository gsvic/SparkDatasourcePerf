import gr.ionio.informatics.SparkDatasourcePerf._
import gr.ionio.informatics.SparkDatasourcePerf.Experiment
import org.apache.spark.sql.catalyst.SQLBuilder

/**
  * Created by vic on 23/9/2016.
  */
object Main extends App{
  Experiment //Initialization

  val sampleExps = (src: String, scale: Int)=> {s"hdfs://master:9000/tpch/tpch_${src}_${scale}/nation"}

  val df = Experiment.sqlContext.read.parquet(sampleExps("parquet", 5))

  val sqlB = new SQLBuilder(df.queryExecution.logical)

  println(sqlB.toSQL)

  /*val results = Seq(1,1,5,20,50).map{ scale =>
    (new Load(Parquet(sampleExps("parquet", scale)))).run().writeToDisk()
  }*/

}

object app{
  Experiment
  val sampleExps = (src: String, table: String, scale: Int)=> {s"hdfs://master:9000/tpch/tpch_${src}_${scale}/${table}"}
  val tables = Seq("customer", "nation", "region", "part", "partsupp", "orders", "lineitem")
  tables.foreach { table =>
    Seq(1, 5, 20, 50).map { scale =>
      (new Load(Parquet(sampleExps("parquet", table, scale)))).run().writeToDisk()
    }
  }
}