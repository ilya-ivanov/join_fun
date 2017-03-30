package join;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import join.RecordSet.IndexType;

public class RecordSetTest {

    @Test
    public void testStoreRecord() throws Exception {
        RecordSet recordSet = new RecordSet(3);
        recordSet.add(new Record("field1", "field2", "field3"));
        recordSet.add(new Record("4", "5", "6"));

        assertEquals(new Record("field1", "field2", "field3"), recordSet.get(0));
        assertEquals(new Record("4", "5", "6"), recordSet.get(1));
    }

    @Test
    public void testFindRecordUsingTreeIndex() throws Exception {
        RecordSet recordSet = new RecordSet(3);
        recordSet.createIndex(1, IndexType.TREE);

        Record record1 = new Record("field1", "field2", "field3");
        Record record2 = new Record("4", "5", "6");
        Record record3 = new Record("1", "5", "7");

        recordSet.add(record1);
        recordSet.add(record2);
        recordSet.add(record3);

        assertThat(recordSet.findRecordsByColumnValue(1, "field2"), contains(record1));
        assertThat(recordSet.findRecordsByColumnValue(1, "5"), containsInAnyOrder(record2, record3));
    }

    @Test
    public void testFindRecordUsingHashIndex() throws Exception {
        RecordSet recordSet = new RecordSet(3);
        recordSet.createIndex(1, IndexType.HASH);

        Record record1 = new Record("field1", "field2", "field3");
        Record record2 = new Record("4", "5", "6");
        Record record3 = new Record("1", "5", "7");

        recordSet.add(record1);
        recordSet.add(record2);
        recordSet.add(record3);

        assertThat(recordSet.findRecordsByColumnValue(1, "field2"), contains(record1));
        assertThat(recordSet.findRecordsByColumnValue(1, "5"), containsInAnyOrder(record2, record3));
    }

    @Test
    public void testFindRecordWithoutIndex() throws Exception {
        RecordSet recordSet = new RecordSet(3);

        Record record1 = new Record("field1", "field2", "field3");
        Record record2 = new Record("4", "5", "6");
        Record record3 = new Record("1", "5", "7");

        recordSet.add(record1);
        recordSet.add(record2);
        recordSet.add(record3);

        assertThat(recordSet.findRecordsByColumnValue(1, "field2"), contains(record1));
        assertThat(recordSet.findRecordsByColumnValue(1, "5"), containsInAnyOrder(record2, record3));
    }

    @Test(expected=InvalidArgumentException.class)
    public void testExceptionWhenAddingORecordWithInvalidSize() throws Exception {
        RecordSet recordSet = new RecordSet(2);

        recordSet.add(new Record("1", "2", "3"));
    }

}
