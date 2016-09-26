import gr.ionio.informatics.SparkDatasourcePerf._

/**
  * Created by vic on 23/9/2016.
  */
object Main extends App{
  val sampleExps = (src: String, scale: Int)=> {s"hdfs://master:9000/tpch/tpch_${src}_${scale}/nation"}
  (new Load(Parquet(sampleExps("parquet", 1)))).run().result

  val results = Seq(1,5,20,50).map{ scale =>
    (new Load(Json(sampleExps("parquet", scale)))).run().writeToDisk()
  }

  results.foreach(println)
  //(new Move(Parquet("hdfs://master:9000/tpch/tpch_parquet_50/customer"), Json("hdfs://master:9000/tpch/tpch_json_50/customer"))).run()
  //Experiment.runExperimentsFromFile("jdbc,192.168.5.20,tpch5,region")
}