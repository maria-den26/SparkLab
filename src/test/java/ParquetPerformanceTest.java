import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static org.apache.spark.sql.functions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetPerformanceTest {

    private static SparkSession spark;
    private static Dataset<Row> sourceData;
    private static final String CSV_PATH = "data/data.csv";
    private static final String PARQUET_UNSORTED_PATH = "target/test-data/ecommerce_unsorted.parquet";
    private static final String PARQUET_SORTED_PATH = "target/test-data/ecommerce_sorted.parquet";
    
    // Количество итераций для усреднения результатов
    private static final int ITERATIONS = 20;

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder()
                .appName("ParquetPerformanceTest")
                .master("local[*]")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.sql.parquet.enableVectorizedReader", "true")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        sourceData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CSV_PATH)
                .cache();
    }

    @AfterAll
    public static void teardown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @FunctionalInterface
    interface DataTransformer {
        Dataset<Row> apply(Dataset<Row> df);
    }

    private static Dataset<Row> transformCustomerStats(Dataset<Row> df) {
        return df.withColumn("TotalPrice",
                        col("UnitPrice").cast("double").multiply(col("Quantity").cast("int")))
                .groupBy("CustomerID")
                .agg(countDistinct("InvoiceNo").alias("InvoiceCnt"),
                        round(sum("TotalPrice"), 4).alias("FullPrice"))
                .withColumn("AvgInvoicePrice", round(expr("FullPrice / InvoiceCnt"), 4));
    }

    private static PerformanceResult measure(String parquetPath, String operationName, 
                                             DataTransformer transformer) {
        // Count одинаковый во всех итерациях, вычисляем один раз
        Dataset<Row> dfOnce = spark.read().parquet(parquetPath);
        Dataset<Row> resultOnce = transformer.apply(dfOnce);
        long count = resultOnce.count();
        
        long totalTime = 0;
        
        for (int i = 0; i < ITERATIONS; i++) {
            // Очистка кэша Spark для полной независимости итераций
            spark.catalog().clearCache();
            
            long startTime = System.currentTimeMillis();
            Dataset<Row> df = spark.read().parquet(parquetPath);
            Dataset<Row> result = transformer.apply(df);
            result.count(); // Выполняем count для запуска вычисления, но не сохраняем
            long elapsed = System.currentTimeMillis() - startTime;
            totalTime += elapsed;
        }
        
        long avgTime = totalTime / ITERATIONS;
        String nameWithAverage = operationName + " (среднее за " + ITERATIONS + " запусков)";
        return new PerformanceResult(nameWithAverage, count, 0, avgTime, 0);
    }

    @Test
    @Order(1)
    @DisplayName("1. Преобразование и запись Parquet без сортировки")
    public void testWriteUnsorted() throws IOException {
        // Count одинаковый во всех итерациях, вычисляем один раз
        Dataset<Row> transformedOnce = transformCustomerStats(sourceData);
        long count = transformedOnce.count();
        
        long totalTime = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            // Полная очистка перед каждой итерацией для независимости
            FileUtilsWrapper.cleanupPath(PARQUET_UNSORTED_PATH);
            spark.catalog().clearCache();
            
            long startTime = System.currentTimeMillis();
            Dataset<Row> transformed = transformCustomerStats(sourceData);
            
            transformed.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(PARQUET_UNSORTED_PATH);
            long writeTime = System.currentTimeMillis() - startTime;
            totalTime += writeTime;
        }

        // Размер файла одинаковый во всех итерациях, вычисляем один раз после цикла
        long fileSize = FileUtilsWrapper.getFolderSize(new File(PARQUET_UNSORTED_PATH));
        long avgTime = totalTime / ITERATIONS;

        PerformanceResult result = new PerformanceResult(
                "Преобразование + запись Parquet БЕЗ сортировки (среднее за " + ITERATIONS + " запусков)",
                count,
                avgTime,
                0,
                fileSize
        );

        result.print();

        Assertions.assertTrue(new File(PARQUET_UNSORTED_PATH).exists(), "Файл Parquet должен быть создан");
    }

    @Test
    @Order(2)
    @DisplayName("2. Преобразование и запись Parquet с сортировкой")
    public void testWriteSorted() throws IOException {
        // Count одинаковый во всех итерациях, вычисляем один раз
        Dataset<Row> transformedOnce = transformCustomerStats(sourceData);
        long count = transformedOnce.count();
        
        long totalTime = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            // Полная очистка перед каждой итерацией для независимости
            FileUtilsWrapper.cleanupPath(PARQUET_SORTED_PATH);
            spark.catalog().clearCache();
            
            long startTime = System.currentTimeMillis();
            Dataset<Row> transformed = transformCustomerStats(sourceData);
            
            transformed.orderBy("CustomerID")
                    .write()
                    .mode(SaveMode.Overwrite)
                    .parquet(PARQUET_SORTED_PATH);
            long writeTime = System.currentTimeMillis() - startTime;
            totalTime += writeTime;
        }

        // Размер файла одинаковый во всех итерациях, вычисляем один раз после цикла
        long fileSize = FileUtilsWrapper.getFolderSize(new File(PARQUET_SORTED_PATH));
        long avgTime = totalTime / ITERATIONS;

        PerformanceResult result = new PerformanceResult(
                "Преобразование + запись Parquet С сортировкой (CustomerID) (среднее за " + ITERATIONS + " запусков)",
                count,
                avgTime,
                0,
                fileSize
        );

        result.print();

        Assertions.assertTrue(new File(PARQUET_SORTED_PATH).exists(), "Файл Parquet должен быть создан");
    }

    @Test
    @Order(3)
    @DisplayName("3. Чтение полного датасета (несортированный)")
    public void testReadFullUnsorted() {
        testAndRecord(PARQUET_UNSORTED_PATH, "Чтение полного датасета (несортированный Parquet)",
                df -> df);
    }

    @Test
    @Order(4)
    @DisplayName("4. Чтение полного датасета (сортированный)")
    public void testReadFullSorted() {
        testAndRecord(PARQUET_SORTED_PATH, "Чтение полного датасета (сортированный Parquet)",
                df -> df);
    }

    @Test
    @Order(5)
    @DisplayName("5. Фильтрация по CustomerID (несортированный)")
    public void testFilterUnsorted() {
        testAndRecord(PARQUET_UNSORTED_PATH, "Фильтрация CustomerID IS NOT NULL (несортированный)",
                df -> df.filter(col("CustomerID").isNotNull()));
    }

    @Test
    @Order(6)
    @DisplayName("6. Фильтрация по CustomerID (сортированный)")
    public void testFilterSorted() {
        testAndRecord(PARQUET_SORTED_PATH, "Фильтрация CustomerID IS NOT NULL (сортированный)",
                df -> df.filter(col("CustomerID").isNotNull()));
    }

    @Test
    @Order(7)
    @DisplayName("7. Фильтрация по диапазону FullPrice (несортированный)")
    public void testMultiFilterUnsorted() {
        testAndRecord(PARQUET_UNSORTED_PATH, "Фильтрация FullPrice > 1000 (несортированный)",
                df -> df.filter(col("FullPrice").gt(1000)));
    }

    @Test
    @Order(8)
    @DisplayName("8. Фильтрация по диапазону FullPrice (сортированный)")
    public void testMultiFilterSorted() {
        testAndRecord(PARQUET_SORTED_PATH, "Фильтрация FullPrice > 1000 (сортированный)",
                df -> df.filter(col("FullPrice").gt(1000)));
    }

    @Test
    @Order(9)
    @DisplayName("9. Сортировка по FullPrice (несортированный)")
    public void testAggregationUnsorted() {
        testAndRecord(PARQUET_UNSORTED_PATH, "Сортировка по FullPrice DESC (несортированный)",
                df -> df.orderBy(desc("FullPrice")).limit(100));
    }

    @Test
    @Order(10)
    @DisplayName("10. Сортировка по FullPrice (сортированный)")
    public void testAggregationSorted() {
        testAndRecord(PARQUET_SORTED_PATH, "Сортировка по FullPrice DESC (сортированный)",
                df -> df.orderBy(desc("FullPrice")).limit(100));
    }

    private void testAndRecord(String path, String name, DataTransformer transformer) {
        PerformanceResult result = measure(path, name, transformer);
        result.print();
        Assertions.assertTrue(result.rowsProcessed > 0);
    }
}

