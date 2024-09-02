import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager, SQLException}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import transform.Transform
import utils.HiveUtils

object HiveExample {
  def main(args: Array[String]): Unit = {
    // Tạo SparkSession với hỗ trợ Hive
    val spark = SparkSession.builder()
      .appName("Extract Multiple DataFrames")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
      .enableHiveSupport() // Bật hỗ trợ Hive
      .getOrCreate()
      
    import spark.implicits._ // Để có Encoder cho String và các loại khác
    val driver = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = "jdbc:mysql://mysql:3306/mydatabase"
    val user = "myuser"
    val password = "mypassword"
    val datalake = "hdfs://namenode:8020/user/datalake"
    // Gọi hàm để lấy DataFrames
    // val df =  spark.read
    //         .format("jdbc")
    //         .option("driver", driver)
    //         .option("url", jdbcUrl)
    //         .option("user", user)
    //         .option("password", password)
    //         .option("dbtable", tableName)
    //         .load()

    // // Truy cập các DataFrame qua tên bảng
    // df.show(10)
    // val (ordersTodayDF, paymentsDF, orderItemsTodayDF, customersDF, sellersDF, productsDF, reviewsDF) = Extractor.extractAllDataUpToDate(spark, jdbcUrl, user, password, driver)
    // ordersTodayDF.show(10)
    // ordersTodayDF.write.mode("overwrite").mode("append").saveAsTable("orders_today")
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val table_names = List(
      "olist_orders_dataset",
      "olist_order_items_dataset", 
      "olist_order_payments_dataset", 
      "olist_customers_dataset", 
      "olist_order_reviews_dataset", 
      "olist_products_dataset", 
      "olist_sellers_dataset"
    )


     // Check all folders are exist?
    val allExist = table_names.forall { table_name =>
      fs.exists(new Path(s"$datalake/$table_name"))
    }

    if (allExist) {
      println("DAILY INGESTION DATA TO DATALAKE")
      val (ordersTodayDF, paymentsDF, orderItemsTodayDF, customersDF, sellersDF, productsDF, reviewsDF) = Extractor.extractAllTodayData(spark, jdbcUrl, user, password, driver)
      if (ordersTodayDF.count() != 0) {
            ordersTodayDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_orders_dataset")  
            paymentsDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_order_payments_dataset")  
            orderItemsTodayDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_order_items_dataset")  
            customersDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_customers_dataset")  
            sellersDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_sellers_dataset")  
            productsDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_products_dataset")  
            reviewsDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_order_reviews_dataset")  
      }
    } else {
        println("RUN PIPELINE FIRSTTIME")
        val (
          ordersTodayDF,
          paymentsDF,
          orderItemsTodayDF, 
          customersDF, 
          sellersDF, 
          productsDF, 
          reviewsDF) = Extractor.extractAllDataUpToDate(spark,
            jdbcUrl,
            user, 
            password, 
            driver
          )

          ordersTodayDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_orders_dataset")  
          paymentsDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_order_payments_dataset")  
          orderItemsTodayDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_order_items_dataset")  
          customersDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_customers_dataset")  
          sellersDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_sellers_dataset")  
          productsDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_products_dataset")  
          reviewsDF.repartition(2).write.mode("append").parquet(f"$datalake/olist_order_reviews_dataset")  
      }

    
    // TRANSFORM DATA THEN LOAD TO HIVE DATA WAREHOUSE
    /*
      1. Load data from datalake 
      2. create fact and dim tables if this is firsttime running
        else upsert dim anđ fact table
      3. save to hive table
    */
    val transformer = new Transform(spark)
    val utils = new HiveUtils(spark)
    val factTableName = "fact_sales"
    val dimCustomerTable = "dim_customer"
    val dimProductTable = "dim_product"
    val dimSellerTable = "dim_seller"
    val dimDateTable = "dim_date"

    // Check if fact table exists
    if (utils.tableExistsInWarehouse(factTableName)) {
      println(s"Table $factTableName is existing. Only load today data")
      
      // Get today's date
      val todayDate = java.time.LocalDate.now().toString

      // Load today's data from datalake
      val ordersTodayDF = transformer.loadTodayOrdersFromDataLake(f"$datalake/olist_orders_dataset", todayDate)
      val orderIdsToday = ordersTodayDF.select("order_id").as[String].collect()  
    
      val orderItemsTodayDF = transformer.loadDataWithStringFilter(f"$datalake/olist_order_items_dataset", "order_id", orderIdsToday)

      val productIdsToday = orderItemsTodayDF.select("product_id").as[String].distinct().collect()
      val sellerIdsToday = orderItemsTodayDF.select("seller_id").as[String].distinct().collect()
      val customerIdsToday = ordersTodayDF.select("customer_id").as[String].collect()

      val paymentsTodayDF = transformer.loadDataWithStringFilter(f"$datalake/olist_order_payments_dataset", "order_id", orderIdsToday)
      val customersTodayDF = transformer.loadDataWithStringFilter(f"$datalake/olist_customers_dataset", "customer_id", customerIdsToday)
      val reviewsTodayDF = transformer.loadDataWithStringFilter(f"$datalake/olist_order_reviews_dataset", "order_id", orderIdsToday)
      val productsTodayDF = transformer.loadDataWithStringFilter(f"$datalake/olist_products_dataset", "product_id", productIdsToday)
      val sellersTodayDF = transformer.loadDataWithStringFilter(f"$datalake/olist_sellers_dataset", "seller_id", sellerIdsToday)

      // Create and update Dimension tables from today's data
      val dimCustomerTodayDF = transformer.createDimCustomer(customersTodayDF)
      transformer.upsertToHiveTable(dimCustomerTodayDF, dimCustomerTable, "customer_id")

      val dimProductTodayDF = transformer.createDimProduct(productsTodayDF)
      transformer.upsertToHiveTable(dimProductTodayDF, dimProductTable, "product_id")

      val dimSellerTodayDF = transformer.createDimSeller(sellersTodayDF)
      transformer.upsertToHiveTable(dimSellerTodayDF, dimSellerTable, "seller_id")

      val dimDateTodayDF = transformer.createDimDate(ordersTodayDF)
      transformer.upsertToHiveTable(dimDateTodayDF, dimDateTable, "date_id")

      // Create a Fact Sales table with today's data
      val factSalesTodayDF = transformer.createFactSalesTable(ordersTodayDF, orderItemsTodayDF, paymentsTodayDF, customersTodayDF, sellersTodayDF, productsTodayDF, reviewsTodayDF)

      // Append new data to existing fact table
      transformer.saveToHiveTable(factSalesTodayDF, factTableName, append = true)

    } else {
      println(s"Table $factTableName is not existing. Load all data")

      // Load all data from datalake
      val ordersDF = transformer.loadDataFromDataLake(f"$datalake/olist_orders_dataset")
      val orderItemsDF = transformer.loadDataFromDataLake(f"$datalake/olist_order_items_dataset")
      val paymentsDF = transformer.loadDataFromDataLake(f"$datalake/olist_order_payments_dataset")
      val customersDF = transformer.loadDataFromDataLake(f"$datalake/olist_customers_dataset")
      val sellersDF = transformer.loadDataFromDataLake(f"$datalake/olist_sellers_dataset")
      val productsDF = transformer.loadDataFromDataLake(f"$datalake/olist_products_dataset")
      val reviewsDF = transformer.loadDataFromDataLake(f"$datalake/olist_order_reviews_dataset")

      ordersDF.printSchema()
      orderItemsDF.printSchema()
      paymentsDF.printSchema()
      customersDF.printSchema()
      sellersDF.printSchema()
      productsDF.printSchema()
      reviewsDF.printSchema()

      // Create Fact Sales table from loaded data
      val factSalesDF = transformer.createFactSalesTable(ordersDF, orderItemsDF, paymentsDF, customersDF, sellersDF, productsDF, reviewsDF)

      // Create Dimension tables
      val dimCustomerDF = transformer.createDimCustomer(customersDF)
      val dimProductDF = transformer.createDimProduct(productsDF)
      val dimSellerDF = transformer.createDimSeller(sellersDF)
      val dimDateDF = transformer.createDimDate(ordersDF)

      // Save dimension tables to Hive
      transformer.saveToHiveTable(dimCustomerDF, dimCustomerTable)
      transformer.saveToHiveTable(dimProductDF, dimProductTable)
      transformer.saveToHiveTable(dimSellerDF, dimSellerTable)
      transformer.saveToHiveTable(dimDateDF, dimDateTable)

      // Save fact table to Hive
      transformer.saveToHiveTable(factSalesDF, factTableName)
    }

    spark.stop()
  }
}


// spark-submit   --class HiveExample   --master spark://spark-master:7077 --deploy-mode client --conf spark.sql.warehouse.dir=hdfs://namenode:8020/user/hive/warehouse    /opt/bitnami/spark/jobs/spark-scala-test.jar
// spark-submit   --class HiveExample   --master spark://spark-master:7077 --deploy-mode client /opt/bitnami/spark/jobs/emcommerce-etl.jar