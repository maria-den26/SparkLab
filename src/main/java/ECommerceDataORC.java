import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.spark.sql.functions.*;

public class ECommerceDataORC {

    public static void main(String[] args) {
        String csvFilePath = "/opt/spark/data/data.csv";

        SparkSession session = SparkSession.builder()
                .appName("ECommerceDataORC")
                .config("spark.master", "local[*]")
                .config("spark.sql.orc.impl", "native")
                .config("spark.sql.orc.enableVectorizedReader", "true")
                .getOrCreate();

        Dataset<Row> df = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .csv(csvFilePath);

        try (PrintWriter fileWriter = new PrintWriter("/opt/spark/data/results_orc.txt")) {
            int rowsCount = (int) df.count();
            fileWriter.println("Размер датасета: " + rowsCount + " строк");

            testOrcWithoutSorting(session, df, fileWriter, rowsCount);
            testOrcWithSorting(session, df, fileWriter, rowsCount);

        } catch (IOException e) {
            e.printStackTrace();
        }

        session.stop();
    }

    private static void testOrcWithoutSorting(SparkSession session, Dataset<Row> df, 
                                             PrintWriter fileWriter, int rowsCount) {
        fileWriter.println("\n=== ORC БЕЗ СОРТИРОВКИ ===");

        int testNumber = 0;
        for (int i = rowsCount; i >= 10000; i = i - 10000) {
            testNumber++;
            fileWriter.println("\nТест " + testNumber + " (" + i + " строк)");

            long writeStart = System.currentTimeMillis();
            df.limit(i).write()
                    .mode(SaveMode.Overwrite)
                    .orc("/opt/spark/data/eCommerceData_unsorted.orc");
            long writeTime = System.currentTimeMillis() - writeStart;

            File file = new File("/opt/spark/data/eCommerceData_unsorted.orc");
            long fileSize = getFolderSize(file);
            fileWriter.println("Размер файла: " + fileSize + " байт (" + (fileSize / 1024.0 / 1024.0) + " MB)");
            fileWriter.println("Время записи: " + writeTime + " мс");

            long readStart = System.currentTimeMillis();
            Dataset<Row> orcDF = session.read().orc("/opt/spark/data/eCommerceData_unsorted.orc");
            orcDF.count();
            long readTime = System.currentTimeMillis() - readStart;
            fileWriter.println("Время чтения всех данных: " + readTime + " мс");

            long filterStart = System.currentTimeMillis();
            Dataset<Row> filteredDF = orcDF.filter(col("Country").equalTo("United Kingdom"));
            long filteredCount = filteredDF.count();
            long filterTime = System.currentTimeMillis() - filterStart;
            fileWriter.println("Время фильтрации по Country='United Kingdom': " + filterTime + " мс (найдено строк: " + filteredCount + ")");
        }
    }

    private static void testOrcWithSorting(SparkSession session, Dataset<Row> df, 
                                          PrintWriter fileWriter, int rowsCount) {
        fileWriter.println("\n=== ORC С СОРТИРОВКОЙ ===");

        int testNumber = 0;
        for (int i = rowsCount; i >= 10000; i = i - 10000) {
            testNumber++;
            fileWriter.println("\nТест " + testNumber + " (" + i + " строк)");

            Dataset<Row> sortedDf = df.limit(i).orderBy("Country", "CustomerID");

            long writeStart = System.currentTimeMillis();
            sortedDf.write()
                    .mode(SaveMode.Overwrite)
                    .orc("/opt/spark/data/eCommerceData_sorted.orc");
            long writeTime = System.currentTimeMillis() - writeStart;

            File file = new File("/opt/spark/data/eCommerceData_sorted.orc");
            long fileSize = getFolderSize(file);
            fileWriter.println("Размер файла: " + fileSize + " байт (" + (fileSize / 1024.0 / 1024.0) + " MB)");
            fileWriter.println("Время записи (с сортировкой): " + writeTime + " мс");

            long readStart = System.currentTimeMillis();
            Dataset<Row> orcDF = session.read().orc("/opt/spark/data/eCommerceData_sorted.orc");
            orcDF.count();
            long readTime = System.currentTimeMillis() - readStart;
            fileWriter.println("Время чтения всех данных: " + readTime + " мс");

            long filterStart = System.currentTimeMillis();
            Dataset<Row> filteredDF = orcDF.filter(col("Country").equalTo("United Kingdom"));
            long filteredCount = filteredDF.count();
            long filterTime = System.currentTimeMillis() - filterStart;
            fileWriter.println("Время фильтрации по Country='United Kingdom': " + filterTime + " мс (найдено строк: " + filteredCount + ")");
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

