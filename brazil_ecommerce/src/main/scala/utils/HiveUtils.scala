package utils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path

class HiveUtils(spark: SparkSession) {
  def tableExistsInDatalake(tableName: String): Boolean = {
    // Use fully qualified table name if necessary
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.exists(new Path(s"hdfs://namenode:8020/user/datalake/$tableName"))
  }

  def tableExistsInWarehouse(tableName: String): Boolean = {
    // Use fully qualified table name if necessary
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.exists(new Path(s"hdfs://namenode:8020/user/hive/warehouse/$tableName"))
  }
}
