package gr.ionio.informatics.SparkDatasourcePerf

/**
  * Created by vic on 19/9/2016.
  */
class Datasource(val path: String)

case class Json(override val path: String) extends Datasource(path)
case class Parquet(override val path: String) extends Datasource(path)
case class JDBC(override val path: String) extends Datasource(path)