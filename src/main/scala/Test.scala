import gr.ionio.informatics.SparkDatasourcePerf._
import gr.ionio.informatics.SparkDatasourcePerf.Experiment

/**
  * Created by vic on 23/9/2016.
  */
object Main extends App{
  Experiment //Initialization

  val sampleExps = (src: String, scale: Int)=> {s"hdfs://master:9000/tpch/tpch_${src}_${scale}/nation"}

  val results = Seq(1,1,5,20,50).map{ scale =>
    (new Load(Parquet(sampleExps("parquet", scale)))).run().writeToDisk()
  }

}