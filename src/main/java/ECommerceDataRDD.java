import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

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

        try (PrintWriter fileWriter = new PrintWriter("/opt/spark/data/results.txt")){

            int rowsCount = (int) df.count();
            fileWriter.println("Размер датасета: " + rowsCount + " строк");

            int j = 0;
            for (int i = rowsCount; i >= 10000; i = i - 10000){
                j++;
                fileWriter.println("Тест " + j);
                df.limit(i).write().mode(SaveMode.Overwrite).parquet("/opt/spark/data/eCommerceData.parquet");

                // Для каждого клиента посчитать:
                //   а. Общее число заказов
                //   б. Общую потраченную сумму
                //   в. Средний чек (TotalPrice на заказ)

                File fileBefore = new File("/opt/spark/data/eCommerceData.parquet");
                long fileSizeBytes = fileBefore.length();
                fileWriter.println("Размер входного файла: " + fileSizeBytes + " байт, " + i + " строк");

                long startTime = System.currentTimeMillis();

                Dataset<Row> parquetFileDF = session.read().parquet("/opt/spark/data/eCommerceData.parquet");

                long endTime = System.currentTimeMillis();
                long delta = endTime - startTime; // Время чтения
                fileWriter.println("Время на чтение: " + delta + " мс");

                Dataset<Row> customersInfoDF = parquetFileDF.withColumn("TotalPrice",
                                col("UnitPrice").cast("double").multiply(col("Quantity").cast("int")))
                        .groupBy("CustomerId")
                        .agg(countDistinct("InvoiceNo").alias("InvoiceCnt"),         //Общее число заказов
                                round(sum("TotalPrice"), 4).alias("FullPrice"))                //Общая потраченная сумма
                        .withColumn("AvgInvoicePrice", round(expr("FullPrice / InvoiceCnt"), 4));  //Средний чек (TotalPrice на заказ)

                customersInfoDF.show();
                int rowsAfter = (int) customersInfoDF.count();

                File fileAfter = new File("/opt/spark/data/customersInfo.parquet");
                fileSizeBytes = fileAfter.length();
                fileWriter.println("Размер выходного файла: " + fileSizeBytes + " байт, " + rowsAfter + " строк");

                startTime = System.currentTimeMillis();

                customersInfoDF.write().mode(SaveMode.Overwrite).parquet("/opt/spark/data/customersInfo.parquet");

                endTime = System.currentTimeMillis();
                delta = endTime - startTime; // Время записи
                fileWriter.println("Время на запись: " + delta + " мс");

            }


        }
        catch (IOException e) {
            e.printStackTrace();
        }


        // топ-5 самых популярных товаров по общему количеству проданных единиц
        df.groupBy("StockCode")
                .agg(sum("Quantity").alias("FullQuantity"))
                .orderBy(desc("FullQuantity"))
                .show(5);


        session.stop();

    }
}