import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
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

        sourceData = sourceData.cache();
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

    private static PerformanceResult measure(String orcPath, String operationName, 
                                             DataTransformer transformer) {
        long startTime = System.currentTimeMillis();
        Dataset<Row> df = spark.read().orc(orcPath);
        Dataset<Row> result = transformer.apply(df);
        long count = result.count();
        long elapsed = System.currentTimeMillis() - startTime;
        return new PerformanceResult(operationName, count, 0, elapsed, 0);
    }

    @Test
    @Order(1)
    @DisplayName("1. Преобразование и запись ORC без сортировки")
    public void testWriteUnsorted() throws IOException {
        FileUtilsWrapper.cleanupPath(ORC_UNSORTED_PATH);

        long startTime = System.currentTimeMillis();
        Dataset<Row> transformed = transformCustomerStats(sourceData);
        long count = transformed.count();
        
        transformed.write()
                .mode(SaveMode.Overwrite)
                .orc(ORC_UNSORTED_PATH);
        long writeTime = System.currentTimeMillis() - startTime;

        long fileSize = FileUtilsWrapper.getFolderSize(new File(ORC_UNSORTED_PATH));

        PerformanceResult result = new PerformanceResult(
                "Преобразование + запись ORC БЕЗ сортировки",
                count,
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
    @DisplayName("2. Преобразование и запись ORC с сортировкой")
    public void testWriteSorted() throws IOException {
        FileUtilsWrapper.cleanupPath(ORC_SORTED_PATH);

        long startTime = System.currentTimeMillis();
        Dataset<Row> transformed = transformCustomerStats(sourceData);
        long count = transformed.count();
        
        transformed.orderBy("CustomerID")
                .write()
                .mode(SaveMode.Overwrite)
                .orc(ORC_SORTED_PATH);
        long writeTime = System.currentTimeMillis() - startTime;

        long fileSize = FileUtilsWrapper.getFolderSize(new File(ORC_SORTED_PATH));

        PerformanceResult result = new PerformanceResult(
                "Преобразование + запись ORC С сортировкой (CustomerID)",
                count,
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
        testAndRecord(ORC_UNSORTED_PATH, "Чтение полного датасета (несортированный ORC)",
                df -> df);
    }

    @Test
    @Order(4)
    @DisplayName("4. Чтение полного датасета (сортированный)")
    public void testReadFullSorted() {
        testAndRecord(ORC_SORTED_PATH, "Чтение полного датасета (сортированный ORC)",
                df -> df);
    }

    @Test
    @Order(5)
    @DisplayName("5. Фильтрация по CustomerID (несортированный)")
    public void testFilterUnsorted() {
        testAndRecord(ORC_UNSORTED_PATH, "Фильтрация CustomerID IS NOT NULL (несортированный)",
                df -> df.filter(col("CustomerID").isNotNull()));
    }

    @Test
    @Order(6)
    @DisplayName("6. Фильтрация по CustomerID (сортированный)")
    public void testFilterSorted() {
        testAndRecord(ORC_SORTED_PATH, "Фильтрация CustomerID IS NOT NULL (сортированный)",
                df -> df.filter(col("CustomerID").isNotNull()));
    }

    @Test
    @Order(7)
    @DisplayName("7. Фильтрация по диапазону FullPrice (несортированный)")
    public void testMultiFilterUnsorted() {
        testAndRecord(ORC_UNSORTED_PATH, "Фильтрация FullPrice > 1000 (несортированный)",
                df -> df.filter(col("FullPrice").gt(1000)));
    }

    @Test
    @Order(8)
    @DisplayName("8. Фильтрация по диапазону FullPrice (сортированный)")
    public void testMultiFilterSorted() {
        testAndRecord(ORC_SORTED_PATH, "Фильтрация FullPrice > 1000 (сортированный)",
                df -> df.filter(col("FullPrice").gt(1000)));
    }

    @Test
    @Order(9)
    @DisplayName("9. Сортировка по FullPrice (несортированный)")
    public void testAggregationUnsorted() {
        testAndRecord(ORC_UNSORTED_PATH, "Сортировка по FullPrice DESC (несортированный)",
                df -> df.orderBy(desc("FullPrice")).limit(100));
    }

    @Test
    @Order(10)
    @DisplayName("10. Сортировка по FullPrice (сортированный)")
    public void testAggregationSorted() {
        testAndRecord(ORC_SORTED_PATH, "Сортировка по FullPrice DESC (сортированный)",
                df -> df.orderBy(desc("FullPrice")).limit(100));
    }

    private void testAndRecord(String path, String name, DataTransformer transformer) {
        PerformanceResult result = measure(path, name, transformer);
        results.add(result);
        result.print();
        Assertions.assertTrue(result.rowsProcessed > 0);
    }
}

