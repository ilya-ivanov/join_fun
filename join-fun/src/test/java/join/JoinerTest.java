package join;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

import org.junit.Test;

public class JoinerTest {

    @Test
    public void testInnerJoin() throws Exception {
        RecordSet table1 = new RecordSet(2);
        table1.add(asList("Ilya Ivanov", "Ulyanovsk, Russia"));
        table1.add(asList("John Smith", "New York, USA"));

        RecordSet table2 = new RecordSet(2);
        table2.add(asList("John Smith", "+1 (123) 456 7890"));

        RecordSet joinResult = Joiner.innerJoin(table1, 0, table2, 0);

        assertEquals(3, joinResult.columnsNum());
        assertEquals(1, joinResult.size());
        assertEquals(asList("John Smith", "New York, USA", "+1 (123) 456 7890"), joinResult.get(0));
    }

    @Test
    public void testInnerJoin_multipleMatch() throws Exception {
        RecordSet table1 = new RecordSet(2);
        table1.add(asList("Ilya Ivanov", "Ulyanovsk, Russia"));
        table1.add(asList("John Smith", "New York, USA"));

        RecordSet table2 = new RecordSet(2);
        table2.add(asList("John Smith", "+1 (123) 456 7890"));
        table2.add(asList("Ilya Ivanov", "+7 (902) 111 2233"));
        table2.add(asList("John Smith", "+1 (800) 987 6543"));

        RecordSet joinResult = Joiner.innerJoin(table1, 0, table2, 0);

        assertEquals(3, joinResult.columnsNum());
        assertEquals(3, joinResult.size());
        assertEquals(asList("Ilya Ivanov", "Ulyanovsk, Russia", "+7 (902) 111 2233"), joinResult.get(0));
        assertEquals(asList("John Smith", "New York, USA", "+1 (123) 456 7890"), joinResult.get(1));
        assertEquals(asList("John Smith", "New York, USA", "+1 (800) 987 6543"), joinResult.get(2));
    }

}
