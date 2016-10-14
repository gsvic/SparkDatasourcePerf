package gr.ionio.informatics.SparkDatasourcePerf

/**
  * @param path The HDFS file full path
  */

abstract class Datasource(val path: String)

case class Json(override val path: String) extends Datasource(path)
case class Parquet(override val path: String) extends Datasource(path)

/**
  * A Postgres data source. @see <a href="https://www.postgresql.org/">Postgres Official Website</a>
  * @param path
  * @param db
  * @param table
  */

case class Postgres(override val path: String, val db: String, val table: String) extends Datasource(path)