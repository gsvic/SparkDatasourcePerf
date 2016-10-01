import gr.ionio.informatics.SparkDatasourcePerf._
import gr.ionio.informatics.SparkDatasourcePerf.Experiment
import org.apache.spark.sql.catalyst.SQLBuilder

/**
  * Created by vic on 23/9/2016.
  */
object Main extends App{
 Experiment.init() //Initialization
 val l = new Load(Postgres("192.168.5.172", "1G", "customer"))

  l.run()


}

object app{
  import gr.ionio.informatics.SparkDatasourcePerf._
  import gr.ionio.informatics.SparkDatasourcePerf.Experiment
  Experiment
  val sampleExps = (src: String, table: String, scale: Int)=> {s"hdfs://master:9000/tpch/${src}/${scale}G/${table}"}
 /* val tables = Seq("customer", "nation", "region", "part", "partsupp", "order", "lineitem")
  tables.foreach { table =>
    Seq(1, 5, 20, 40).map { scale =>
      (new Load(Json(sampleExps("json", table, scale)))).run().writeToDisk()
    }
  }*/

  println((new Load(Postgres("192.168.5.172", "1G", "customer"))).run())
}