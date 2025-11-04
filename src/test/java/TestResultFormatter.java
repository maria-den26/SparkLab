import java.util.ArrayList;
import java.util.List;

public class TestResultFormatter {

    private List<String[]> rows = new ArrayList<>();
    private String[] headers;
    private int[] columnWidths;

    public TestResultFormatter(String... headers) {
        this.headers = headers;
        this.columnWidths = new int[headers.length];
        for (int i = 0; i < headers.length; i++) {
            columnWidths[i] = headers[i].length();
        }
    }

    public void addRow(String... cells) {
        if (cells.length != headers.length) {
            throw new IllegalArgumentException("Number of cells must match number of headers");
        }
        rows.add(cells);
        for (int i = 0; i < cells.length; i++) {
            columnWidths[i] = Math.max(columnWidths[i], cells[i].length());
        }
    }

    public void print() {
        printSeparator();
        printRow(headers);
        printSeparator();
        for (String[] row : rows) {
            printRow(row);
        }
        printSeparator();
    }

    private void printSeparator() {
        StringBuilder sb = new StringBuilder("+");
        for (int width : columnWidths) {
            sb.append(repeatChar('-', width + 2));
            sb.append("+");
        }
        System.out.println(sb.toString());
    }

    private void printRow(String[] cells) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < cells.length; i++) {
            sb.append(" ");
            sb.append(padRight(cells[i], columnWidths[i]));
            sb.append(" |");
        }
        System.out.println(sb.toString());
    }

    private String padRight(String s, int width) {
        return String.format("%-" + width + "s", s);
    }

    private String repeatChar(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}

