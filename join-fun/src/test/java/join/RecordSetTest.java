package join;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

import org.junit.Test;

public class RecordSetTest {

    @Test
    public void testStoreRecord() throws Exception {
        RecordSet recordSet = new RecordSet(3);
        recordSet.add(asList("field1", "field2", "field3"));
        recordSet.add(asList("4", "5", "6"));

        assertEquals(asList("field1", "field2", "field3"), recordSet.get(0));
        assertEquals(asList("4", "5", "6"), recordSet.get(1));
    }

    @Test(expected=InvalidArgumentException.class)
    public void testExceptionWhenAddingORecordWithInvalidSize() throws Exception {
        RecordSet recordSet = new RecordSet(2);

        recordSet.add(asList("1", "2", "3"));
    }

}
