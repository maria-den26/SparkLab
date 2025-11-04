import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class FileUtilsWrapper {
    public static long getFolderSize(File folder) {
        return FileUtils.sizeOfDirectory(folder);
    }

    public static void cleanupPath(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                FileUtils.deleteDirectory(file);
            } else {
                FileUtils.delete(file);
            }
        }
    }
}
