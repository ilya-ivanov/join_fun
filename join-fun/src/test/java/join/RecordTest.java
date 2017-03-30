package join;

import static org.junit.Assert.*;

import org.junit.Test;

public class RecordTest {

    @Test
    public void testCompare() {
        Record record1 = new Record("val1", "val2");
        Record record2 = new Record("val1", "val0");

        Record equalRecord = new Record("val1", "val2");

        assertTrue(record1.compareTo(record2) > 0);
        assertTrue(record2.compareTo(record1) < 0);
        assertTrue(record1.compareTo(equalRecord) == 0);
    }

}
