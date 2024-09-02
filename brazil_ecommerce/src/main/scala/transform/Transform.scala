    // TRANSFORM DATA
    /*
      1. Load data from datalake (HDFS)
      2. create fact and dim tables if this is first time running
        else upsert dim anđ fact table
      3. save to hive table
    */

package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Transform(spark: SparkSession) {
  
    // Function to load all data from datalake
    def loadDataFromDataLake(tablePath: String): DataFrame = {
        spark.read.parquet(tablePath)
    }

    // Function to load today data from datalake
    def loadTodayOrdersFromDataLake(tablePath: String, todayDate: String): DataFrame = {
        spark.read.parquet(tablePath).filter(col("order_purchase_timestamp").startsWith(todayDate))
    }

    def loadDataWithStringFilter(tablePath: String, columnName: String, filterValues: Array[String]): DataFrame = {
        spark.read.parquet(tablePath).filter(col(columnName).isin(filterValues: _*))
    }

    // Func to create facts table from loaded dataframe
    def createFactSalesTable(
        ordersDF: DataFrame,
        orderItemsDF: DataFrame,
        paymentsDF: DataFrame,
        customersDF: DataFrame,
        sellersDF: DataFrame,
        productsDF: DataFrame,
        reviewsDF: DataFrame
    ): DataFrame = {
        
        // Join tables
        val factSalesDF = ordersDF
        .join(orderItemsDF, ordersDF("order_id") === orderItemsDF("order_id"), "inner")
        .join(customersDF, ordersDF("customer_id") === customersDF("customer_id"), "inner")
        .join(sellersDF, orderItemsDF("seller_id") === sellersDF("seller_id"), "inner")
        .join(productsDF, orderItemsDF("product_id") === productsDF("product_id"), "inner")
        .join(paymentsDF, ordersDF("order_id") === paymentsDF("order_id"), "left_outer")
        .join(reviewsDF, ordersDF("order_id") === reviewsDF("order_id"), "left_outer")
        .select(
            ordersDF("order_purchase_timestamp").alias("date_id"),
            ordersDF("order_id"),
            orderItemsDF("product_id"),
            customersDF("customer_id"),
            sellersDF("seller_id"),
            orderItemsDF("price").alias("sales_amount"),
            orderItemsDF("freight_value"),
            paymentsDF("payment_value").alias("total_amount")
        )

        factSalesDF
    }

    // Fuc to save DataFrame to Hive table
    def saveToHiveTable(df: DataFrame, tableName: String, append: Boolean = false): Unit = {
        val mode = if (append) "append" else "overwrite"
        df.write.mode(mode).saveAsTable(tableName)
    }

    // func to check if table exists
    def isHiveTableExists(tableName: String): Boolean = {
        spark.catalog.tableExists(tableName)
    }
    
    def upsertToHiveTable(newDF: DataFrame, tableName: String, primaryKey: String): Unit = {
        import spark.implicits._

        // Read existing table from Hive
        val existingDF = spark.table(tableName)

        // Find rows in newDF that are not present in existingDF (using left anti join)
        val nonDuplicateNewDF = newDF.join(existingDF, Seq(primaryKey), "left_anti")

        // Append non-duplicate rows to the Hive table
        nonDuplicateNewDF.write.mode("append").saveAsTable(tableName)
    }

    def createDimCustomer(customersDF: DataFrame): DataFrame = {
        customersDF.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix"),
        col("customer_city"),
        col("customer_state")
        ).distinct()
    }

    def createDimProduct(productsDF: DataFrame): DataFrame = {
        productsDF.select(
            col("product_id"),
            col("product_category_name"),
            col("product_name_lenght"),
            col("product_description_lenght")
        )
        .distinct()
    }

    def createDimSeller(sellersDF: DataFrame): DataFrame = {
        sellersDF.select(
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state")
        ).distinct()
    }

    def createDimDate(ordersDF: DataFrame): DataFrame = {
        val datesDF = ordersDF.select(col("order_purchase_timestamp").alias("full_date"))
        .withColumn("date_id", date_format(col("full_date"), "yyyyMMdd"))
        .withColumn("day", dayofmonth(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("year", year(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("week_number", weekofyear(col("full_date")))
        .withColumn("is_weekend", expr("CASE WHEN dayofweek(full_date) IN (1, 7) THEN 1 ELSE 0 END"))
        .distinct()

        datesDF
    }
}
