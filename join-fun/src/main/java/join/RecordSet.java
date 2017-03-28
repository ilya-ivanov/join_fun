package join;

import java.util.ArrayList;
import java.util.List;

/**
 * Records collection. Enforcing equal columns number in all records
 * @author Ilya Ivanov
 */
public class RecordSet extends ArrayList<List<String>> {

    private int columnsNum;

    public RecordSet(int columns) {
        this.columnsNum = columns;
    }

    public boolean add(List<String> record) {
        if (record.size() != columnsNum) {
            throw new InvalidArgumentException("Invalid columns number in record");
        }

        super.add(record);

        return true;
    }

    public int columnsNum() {
        return columnsNum;
    }

}
