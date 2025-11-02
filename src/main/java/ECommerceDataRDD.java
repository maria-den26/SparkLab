import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.*;

public class ECommerceDataRDD {

    public static void main(String[] args) {
        String csvFilePath = "/opt/spark/data/data.csv";

        SparkSession session = SparkSession.builder()
                .appName("ECommerceData")
                .config("spark.master", "local[*]")
                .getOrCreate();

        Dataset<Row> df = session.read().option("delimiter", ",").option("header", "true").csv(csvFilePath);
        df.show();
        // топ-5 самых популярных товаров по общему количеству проданных единиц
        df.groupBy("StockCode")
                .agg(sum("Quantity").alias("FullQuantity"))
                .orderBy(desc("FullQuantity"))
                .show(5);


        // Для каждого клиента посчитать:
        //   а. Общее число заказов
        //   б. Общую потраченную сумму
        //   в. Средний чек (TotalPrice на заказ)

        df.withColumn("TotalPrice",
                        col("UnitPrice").cast("double").multiply(col("Quantity").cast("int")))
                .groupBy("CustomerId")
                .agg(countDistinct("InvoiceNo").alias("InvoiceCnt"),         //Общее число заказов
                        round(sum("TotalPrice"), 4).alias("FullPrice"))                //Общая потраченная сумма
                .withColumn("AvgInvoicePrice", round(expr("FullPrice / InvoiceCnt"), 4))  //Средний чек (TotalPrice на заказ)
                .show();

        session.stop();

    }
}