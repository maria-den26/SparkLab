import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ORCPerformanceTest {

    private static SparkSession spark;
    private static Dataset<Row> sourceData;
    private static final String CSV_PATH = "data/data.csv";
    private static final String ORC_UNSORTED_PATH = "target/test-data/ecommerce_unsorted.orc";
    private static final String ORC_SORTED_PATH = "target/test-data/ecommerce_sorted.orc";
    
    private static List<PerformanceResult> results = new ArrayList<>();

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder()
                .appName("ORCPerformanceTest")
                .master("local[*]")
                .config("spark.sql.orc.impl", "native")
                .config("spark.sql.orc.enableVectorizedReader", "true")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        sourceData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH);

        sourceData.cache();
    }

    @AfterAll
    public static void teardown() {
        printSummary();
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @Order(1)
    @DisplayName("1. Запись ORC без сортировки")
    public void testWriteUnsorted() throws IOException {
        cleanupPath(ORC_UNSORTED_PATH);

        long startTime = System.currentTimeMillis();
        sourceData.write()
                .mode(SaveMode.Overwrite)
                .orc(ORC_UNSORTED_PATH);
        long writeTime = System.currentTimeMillis() - startTime;

        long fileSize = getFolderSize(new File(ORC_UNSORTED_PATH));

        PerformanceResult result = new PerformanceResult(
                "Запись ORC БЕЗ сортировки",
                sourceData.count(),
                writeTime,
                0,
                fileSize
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(new File(ORC_UNSORTED_PATH).exists(), "Файл ORC должен быть создан");
    }

    @Test
    @Order(2)
    @DisplayName("2. Запись ORC с сортировкой")
    public void testWriteSorted() throws IOException {
        cleanupPath(ORC_SORTED_PATH);

        long startTime = System.currentTimeMillis();
        sourceData.orderBy("Country", "CustomerID")
                .write()
                .mode(SaveMode.Overwrite)
                .orc(ORC_SORTED_PATH);
        long writeTime = System.currentTimeMillis() - startTime;

        long fileSize = getFolderSize(new File(ORC_SORTED_PATH));

        PerformanceResult result = new PerformanceResult(
                "Запись ORC С сортировкой (Country, CustomerID)",
                sourceData.count(),
                writeTime,
                0,
                fileSize
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(new File(ORC_SORTED_PATH).exists(), "Файл ORC должен быть создан");
    }

    @Test
    @Order(3)
    @DisplayName("3. Чтение полного датасета (несортированный)")
    public void testReadFullUnsorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_UNSORTED_PATH);
        long count = df.count();
        long readTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Чтение ВСЕХ данных (несортированный ORC)",
                count,
                0,
                readTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть прочитаны данные");
    }

    @Test
    @Order(4)
    @DisplayName("4. Чтение полного датасета (сортированный)")
    public void testReadFullSorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_SORTED_PATH);
        long count = df.count();
        long readTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Чтение ВСЕХ данных (сортированный ORC)",
                count,
                0,
                readTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть прочитаны данные");
    }

    @Test
    @Order(5)
    @DisplayName("5. Фильтрация по Country (несортированный)")
    public void testFilterUnsorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_UNSORTED_PATH);
        Dataset<Row> filtered = df.filter(col("Country").equalTo("United Kingdom"));
        long count = filtered.count();
        long filterTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Фильтрация Country='United Kingdom' (несортированный)",
                count,
                0,
                filterTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть найдены записи для United Kingdom");
    }

    @Test
    @Order(6)
    @DisplayName("6. Фильтрация по Country (сортированный)")
    public void testFilterSorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_SORTED_PATH);
        Dataset<Row> filtered = df.filter(col("Country").equalTo("United Kingdom"));
        long count = filtered.count();
        long filterTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Фильтрация Country='United Kingdom' (сортированный)",
                count,
                0,
                filterTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть найдены записи для United Kingdom");
    }

    @Test
    @Order(7)
    @DisplayName("7. Фильтрация по нескольким полям (несортированный)")
    public void testMultiFilterUnsorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_UNSORTED_PATH);
        Dataset<Row> filtered = df.filter(
                col("Country").equalTo("United Kingdom")
                        .and(col("CustomerID").isNotNull())
        );
        long count = filtered.count();
        long filterTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Фильтрация Country='UK' AND CustomerID IS NOT NULL (несортированный)",
                count,
                0,
                filterTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть найдены записи");
    }

    @Test
    @Order(8)
    @DisplayName("8. Фильтрация по нескольким полям (сортированный)")
    public void testMultiFilterSorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_SORTED_PATH);
        Dataset<Row> filtered = df.filter(
                col("Country").equalTo("United Kingdom")
                        .and(col("CustomerID").isNotNull())
        );
        long count = filtered.count();
        long filterTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Фильтрация Country='UK' AND CustomerID IS NOT NULL (сортированный)",
                count,
                0,
                filterTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть найдены записи");
    }

    @Test
    @Order(9)
    @DisplayName("9. Агрегация данных (несортированный)")
    public void testAggregationUnsorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_UNSORTED_PATH);
        Dataset<Row> aggregated = df.groupBy("Country")
                .agg(
                        count("*").alias("OrderCount"),
                        sum(col("Quantity").cast("int")).alias("TotalQuantity")
                )
                .orderBy(desc("OrderCount"));
        long count = aggregated.count();
        long aggTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Агрегация по Country (несортированный)",
                count,
                0,
                aggTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть агрегированные данные");
    }

    @Test
    @Order(10)
    @DisplayName("10. Агрегация данных (сортированный)")
    public void testAggregationSorted() {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(ORC_SORTED_PATH);
        Dataset<Row> aggregated = df.groupBy("Country")
                .agg(
                        count("*").alias("OrderCount"),
                        sum(col("Quantity").cast("int")).alias("TotalQuantity")
                )
                .orderBy(desc("OrderCount"));
        long count = aggregated.count();
        long aggTime = System.currentTimeMillis() - startTime;

        PerformanceResult result = new PerformanceResult(
                "Агрегация по Country (сортированный)",
                count,
                0,
                aggTime,
                0
        );
        results.add(result);
        result.print();

        Assertions.assertTrue(count > 0, "Должны быть агрегированные данные");
    }

    private static void printSummary() {
        System.out.println("\n--- ЗАПИСЬ ДАННЫХ ---");
        TestResultFormatter writeTable = new TestResultFormatter("Операция", "Время (мс)", "Размер (MB)");
        results.stream()
                .filter(r -> r.operation.contains("Запись"))
                .forEach(r -> writeTable.addRow(
                        r.operation,
                        String.valueOf(r.writeTime),
                        String.format("%.2f", r.fileSize / 1024.0 / 1024.0)
                ));
        writeTable.print();

        System.out.println("\n--- ЧТЕНИЕ ПОЛНЫХ ДАННЫХ ---");
        TestResultFormatter readTable = new TestResultFormatter("Операция", "Время (мс)", "Строк");
        results.stream()
                .filter(r -> r.operation.contains("ВСЕХ данных"))
                .forEach(r -> readTable.addRow(
                        r.operation,
                        String.valueOf(r.readTime),
                        String.valueOf(r.rowsProcessed)
                ));
        readTable.print();

        System.out.println("\n--- ФИЛЬТРАЦИЯ ---");
        TestResultFormatter filterTable = new TestResultFormatter("Операция", "Время (мс)", "Найдено строк");
        results.stream()
                .filter(r -> r.operation.contains("Фильтрация"))
                .forEach(r -> filterTable.addRow(
                        r.operation,
                        String.valueOf(r.readTime),
                        String.valueOf(r.rowsProcessed)
                ));
        filterTable.print();

        System.out.println("\n--- АГРЕГАЦИЯ ---");
        TestResultFormatter aggTable = new TestResultFormatter("Операция", "Время (мс)", "Групп");
        results.stream()
                .filter(r -> r.operation.contains("Агрегация"))
                .forEach(r -> aggTable.addRow(
                        r.operation,
                        String.valueOf(r.readTime),
                        String.valueOf(r.rowsProcessed)
                ));
        aggTable.print();
    }

    private static void cleanupPath(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                deleteDirectory(file);
            } else {
                Files.delete(Paths.get(path));
            }
        }
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
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

    private static class PerformanceResult {
        String operation;
        long rowsProcessed;
        long writeTime;
        long readTime;
        long fileSize;

        PerformanceResult(String operation, long rowsProcessed, long writeTime, long readTime, long fileSize) {
            this.operation = operation;
            this.rowsProcessed = rowsProcessed;
            this.writeTime = writeTime;
            this.readTime = readTime;
            this.fileSize = fileSize;
        }

        void print() {
            System.out.println("\n--- " + operation + " ---");
            if (writeTime > 0) {
                System.out.println("Время записи: " + writeTime + " мс");
            }
            if (readTime > 0) {
                System.out.println("Время выполнения: " + readTime + " мс");
            }
            if (fileSize > 0) {
                System.out.printf("Размер файла: %d байт (%.2f MB)%n", fileSize, fileSize / 1024.0 / 1024.0);
            }
            System.out.println("Обработано строк: " + rowsProcessed);
        }
    }
}

