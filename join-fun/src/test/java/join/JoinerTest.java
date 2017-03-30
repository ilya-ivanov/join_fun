package join;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import join.RecordSet.IndexType;

public class JoinerTest {

    @Test
    public void testInnerJoin() throws Exception {
        RecordSet table1 = new RecordSet(2);
        table1.add(new Record("Ilya Ivanov", "Ulyanovsk, Russia"));
        table1.add(new Record("John Smith", "New York, USA"));

        RecordSet table2 = new RecordSet(2);
        table2.add(new Record("John Smith", "+1 (123) 456 7890"));

        // will join by first fields
        Joiner joiner = new Joiner(0,  0);
        RecordSet joinResult = joiner.innerJoin(table1, table2);

        assertEquals(3, joinResult.columnsNum());
        assertEquals(1, joinResult.size());
        assertEquals(new Record("John Smith", "New York, USA", "+1 (123) 456 7890"), joinResult.get(0));
    }

    @Test
    public void testInnerJoin_multipleMatch() throws Exception {
        RecordSet table1 = new RecordSet(2);
        table1.add(new Record("Ilya Ivanov", "Ulyanovsk, Russia"));
        table1.add(new Record("John Smith", "New York, USA"));

        RecordSet table2 = new RecordSet(2);
        table2.add(new Record("John Smith", "+1 (123) 456 7890"));
        table2.add(new Record("Ilya Ivanov", "+7 (902) 111 2233"));
        table2.add(new Record("John Smith", "+1 (800) 987 6543"));

        Joiner joiner = new Joiner(0,  0);
        RecordSet joinResult = joiner.innerJoin(table1, table2);

        assertEquals(3, joinResult.columnsNum());
        assertEquals(3, joinResult.size());
        assertEquals(new Record("Ilya Ivanov", "Ulyanovsk, Russia", "+7 (902) 111 2233"), joinResult.get(0));
        assertEquals(new Record("John Smith", "New York, USA", "+1 (123) 456 7890"), joinResult.get(1));
        assertEquals(new Record("John Smith", "New York, USA", "+1 (800) 987 6543"), joinResult.get(2));
    }

    @Test
    public void testInnerJoin_withTreeIndex() throws Exception {
        RecordSet table1 = new RecordSet(2);
        table1.createIndex(0, IndexType.TREE);

        table1.add(new Record("Ilya Ivanov", "Ulyanovsk, Russia"));
        table1.add(new Record("John Smith", "New York, USA"));

        RecordSet table2 = new RecordSet(2);
        table2.add(new Record("John Smith", "+1 (123) 456 7890"));
        table2.add(new Record("Ilya Ivanov", "+7 (902) 111 2233"));
        table2.add(new Record("John Smith", "+1 (800) 987 6543"));

        Joiner joiner = new Joiner(0,  0);
        RecordSet joinResult = joiner.innerJoin(table1, table2);

        assertEquals(3, joinResult.columnsNum());
        assertEquals(3, joinResult.size());
        assertEquals(new Record("Ilya Ivanov", "Ulyanovsk, Russia", "+7 (902) 111 2233"), joinResult.get(0));
        assertEquals(new Record("John Smith", "New York, USA", "+1 (123) 456 7890"), joinResult.get(1));
        assertEquals(new Record("John Smith", "New York, USA", "+1 (800) 987 6543"), joinResult.get(2));
    }


    @Test
    public void comparePerformance() throws IOException {
        File file1 = new File(JoinerTest.class.getResource("/table1").getFile());
        File file2 = new File(JoinerTest.class.getResource("/table2").getFile());

        RecordSet table1 = new RecordSet(3);
        FileUtils.readLines(file1, Charset.defaultCharset())
            .stream().forEach(line -> table1.add(new Record(line.split(","))));

        assertTrue(table1.size() > 0);

        RecordSet table2 = new RecordSet(3);
        FileUtils.readLines(file2, Charset.defaultCharset())
            .stream().forEach(line -> table2.add(new Record(line.split(","))));

        assertTrue(table2.size() > 0);

        Collections.shuffle(table1);
        Collections.shuffle(table2);

        Joiner joiner = new Joiner(2, 2);

        printRecordSet(joiner.innerJoin(table1, table2));
        System.out.println();

        // Join with no index

        System.out.println("Joining without index...");
        runJoinAndLogTime(table1, table2, joiner);

        // Join with hash index

        table1.createIndex(2, IndexType.HASH);
        table2.createIndex(2, IndexType.HASH);

        System.out.println("Joining with hash index...");
        runJoinAndLogTime(table1, table2, joiner);

        // Join with tree index

        table1.createIndex(2, IndexType.TREE);
        table2.createIndex(2, IndexType.TREE);

        System.out.println("Joining with hash index...");
        runJoinAndLogTime(table1, table2, joiner);
    }

    private void printRecordSet(RecordSet recordSet) {
        for (Record rec : recordSet) {
            System.out.println(rec);
        }
    }

    private void runJoinAndLogTime(RecordSet table1, RecordSet table2, Joiner joiner) {
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            joiner.innerJoin(table1, table2);
        }

        long finishTime = System.currentTimeMillis();
        System.out.println("Finished in " + (finishTime - startTime) + " ms \n");
    }

}
