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
        //df.show();

        try (PrintWriter fileWriter = new PrintWriter("/opt/spark/data/results_parquet.txt")){

            int rowsCount = (int) df.count();
            fileWriter.println("Размер датасета: " + rowsCount + " строк");

            testParquet(session, df, fileWriter, rowsCount);

        }
        catch (IOException e) {
            e.printStackTrace();
        }


        session.stop();

    }

    private static void testParquet(SparkSession session, Dataset<Row> df,
                                              PrintWriter fileWriter, int rowsCount){

        int testNumber = 0;
        for (int i = rowsCount; i >= 10000; i = i - 10000){
            testNumber++;
            fileWriter.println("\nТест " + testNumber);

            long writeStart = System.currentTimeMillis();
            df.limit(i)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .parquet("/opt/spark/data/eCommerceData.parquet");
            long writeTime = System.currentTimeMillis() - writeStart;

            // Для каждого клиента посчитать:
            //   а. Общее число заказов
            //   б. Общую потраченную сумму
            //   в. Средний чек (TotalPrice на заказ)

            File fileBefore = new File("/opt/spark/data/eCommerceData.parquet");
            long fileSizeBytes = getFolderSize(fileBefore);
            fileWriter.println("Размер входного файла: " + fileSizeBytes + " байт (" + (fileSizeBytes / 1024.0 / 1024.0) + " MB), " + i + " строк");
            fileWriter.println("Время записи: " + writeTime + " мс");

            long readStart = System.currentTimeMillis();
            Dataset<Row> parquetFileDF = session.read().parquet("/opt/spark/data/eCommerceData.parquet");
            parquetFileDF.count();
            long readTime = System.currentTimeMillis() - readStart;
            fileWriter.println("Время чтения всех данных: " + readTime + " мс");

            long selectStart = System.currentTimeMillis();
            Dataset<Row> customersInfoDF = parquetFileDF.withColumn("TotalPrice",
                            col("UnitPrice").cast("double").multiply(col("Quantity").cast("int")))
                    .groupBy("CustomerId")
                    .agg(countDistinct("InvoiceNo").alias("InvoiceCnt"),         //Общее число заказов
                            round(sum("TotalPrice"), 4).alias("FullPrice"))                //Общая потраченная сумма
                    .withColumn("AvgInvoicePrice", round(expr("FullPrice / InvoiceCnt"), 4));  //Средний чек (TotalPrice на заказ)

            int rowsAfter = (int) customersInfoDF.count();
            long selectTime = System.currentTimeMillis() - selectStart;

            fileWriter.println("Время чтения и преобразования данных: " + selectTime + " мс");

            writeStart = System.currentTimeMillis();
            customersInfoDF.write().mode(SaveMode.Overwrite).parquet("/opt/spark/data/customersInfo.parquet");
            writeTime = System.currentTimeMillis() - writeStart; // Время записи

            File fileAfter = new File("/opt/spark/data/customersInfo.parquet");
            fileSizeBytes = getFolderSize(fileAfter);
            fileWriter.println("Размер выходного файла: " + fileSizeBytes + " байт (" + (fileSizeBytes / 1024.0 / 1024.0) + " MB), " + rowsAfter + " строк");
            fileWriter.println("Время записи: " + writeTime + " мс");

        }

    }

    private static long getFolderSize(File folder) {
        long size = 0;
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        size += file.length();
                    } else {
                        size += getFolderSize(file);
                    }
                }
            }
        } else {
            size = folder.length();
        }
        return size;
    }
}