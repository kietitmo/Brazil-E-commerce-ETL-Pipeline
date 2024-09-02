/*
First, check if there is any data table in the datalake.
If not, it means the pipeline has not run yet => proceed to load all data.

If there is data in the datalake, it means the pipeline has run => only get data for today to append to the datalake.

So in class Extractor there are a total of 2 main functions:
    1. extractAllDataUpToDate: extract all data from source (mysql)
    2. extractAllTodayData: only extract data of today
*/

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Extractor {
  // Function to extract full data up to today from the given MySQL table
    def extractAllDataUpToDate(
        spark: SparkSession,
        jdbcUrl: String, 
        user: String, 
        password: String, 
        driver: String
        ):  (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
        import spark.implicits._
        def readTable(tableName: String): DataFrame = {
            spark.read
                .format("jdbc")
                .option("driver", driver)
                .option("url", jdbcUrl)
                .option("dbtable", tableName)
                .option("user", user)
                .option("password", password)
                .load()
        }

         // Return a map of table name to DataFrame for further processing
        val ordersDF = readTable("olist_orders_dataset")
        val paymentsDF = readTable("olist_order_payments_dataset")
        val orderItemsDF = readTable("olist_order_items_dataset")
        val customersDF = readTable("olist_customers_dataset")
        val sellersDF = readTable("olist_sellers_dataset")
        val productsDF = readTable("olist_products_dataset")
        val reviewsDF = readTable("olist_order_reviews_dataset")
        
        (ordersDF, paymentsDF, orderItemsDF, customersDF, sellersDF, productsDF, reviewsDF)
        
    }

        // Function to extract today's data from all tables
    def extractAllTodayData(
        spark: SparkSession,
        jdbcUrl: String,
        user: String,
        password: String,
        driver: String
    ): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
        import spark.implicits._ // Để có Encoder cho String và các loại khác
     // Helper function to read table with date filter
        def readTableWithDateFilter(tableName: String, dateColumn: String, dateValue: java.sql.Date, driver: String): DataFrame = {
        spark.read
            .format("jdbc")
            .option("driver", driver)
            .option("url", jdbcUrl)
            .option("dbtable", tableName)
            .option("user", user)
            .option("password", password)
            .load()
            .filter(col(dateColumn).cast("date") === lit(dateValue))
        }

        // Helper function to read table with string filter
        def readTableWithStringFilter(tableName: String, columnName: String, driver: String, filterValues: Array[String]): DataFrame = {
        spark.read
            .format("jdbc")
            .option("driver", driver)
            .option("url", jdbcUrl)
            .option("dbtable", tableName)
            .option("user", user)
            .option("password", password)
            .load()
            .filter(col(columnName).isin(filterValues: _*))
        }

        // Get today's date
        val today = new java.sql.Date(System.currentTimeMillis())
        val driver = "com.mysql.cj.jdbc.Driver"

        // Extract data for orders placed today
        val ordersTodayDF = readTableWithDateFilter("olist_orders_dataset", "order_purchase_timestamp", today, driver)

        // Check if there is any order today
        if (ordersTodayDF.count() == 0) {
            println("No orders found for today's date. Exiting the extraction process.")
            val emptyDF = spark.emptyDataFrame
            (
                emptyDF, // ordersTodayDF
                emptyDF, // paymentsDF
                emptyDF, // orderItemsTodayDF
                emptyDF, // customersDF
                emptyDF, // sellersDF
                emptyDF, // productsDF
                emptyDF  // reviewsDF
            )
        } else {
            // Extract data for order items related to today's orders
            val orderIdsToday = ordersTodayDF.select("order_id").as[String].collect()
            val orderItemsTodayDF = readTableWithStringFilter("olist_order_items_dataset", "order_id", driver, orderIdsToday)

            // Extract product IDs and seller IDs
            val productIdsToday = orderItemsTodayDF.select("product_id").as[String].distinct().collect()
            val sellerIdsToday = orderItemsTodayDF.select("seller_id").as[String].distinct().collect()
            val customerIdsToday = ordersTodayDF.select("customer_id").as[String].collect()

            // Extract data for related tables using the extracted IDs
            val paymentsDF = readTableWithStringFilter("olist_order_payments_dataset", "order_id", driver, orderIdsToday)
            val customersDF = readTableWithStringFilter("olist_customers_dataset", "customer_id", driver, customerIdsToday)
            val reviewsDF = readTableWithStringFilter("olist_order_reviews_dataset", "order_id", driver, orderIdsToday)
            val productsDF = readTableWithStringFilter("olist_products_dataset", "product_id", driver, productIdsToday)
            val sellersDF = readTableWithStringFilter("olist_sellers_dataset", "seller_id", driver, sellerIdsToday)

            // Return a map of table name to DataFrame for further processing
            (
                ordersTodayDF,
                paymentsDF,
                orderItemsTodayDF,
                customersDF,
                sellersDF,
                productsDF,
                reviewsDF
            )
        }
    }
}