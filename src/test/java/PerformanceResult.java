public class PerformanceResult {
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