package join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * Records collection. Enforcing equal columns number in all records
 * @author Ilya Ivanov
 */
public class RecordSet extends ArrayList<Record> {

    public static enum IndexType {TREE, HASH}

    private int columnsNum;

    private Multimap<String, Record>[] indexes;

    public RecordSet(int columns) {
        this.columnsNum = columns;
        indexes = new Multimap[columns];
    }

    public boolean add(Record record) {
        if (record.size() != columnsNum) {
            throw new InvalidArgumentException("Invalid columns number in record");
        }

        super.add(record);

        indexRecord(record);

        return true;
    }

    private void indexRecord(Record record) {
        for (int i = 0; i < indexes.length; i++) {
            if (indexes[i] != null) {
                indexes[i].put(record.get(i), record);
            }
        }
    }

    public int columnsNum() {
        return columnsNum;
    }

    public void createIndex(int column, IndexType type) {
        switch(type) {
            case TREE:
                indexes[column] = TreeMultimap.<String, Record>create();
                forEach(this::indexRecord);
                break;
            case HASH:
                indexes[column] = HashMultimap.<String, Record>create();
                forEach(this::indexRecord);
                break;
        }
    }

    public Collection<Record> findRecordsByColumnValue(int column, String value) {
        if (indexes[column] != null) {
            return indexes[column].get(value);
        } else {
            // if there's no index do plain search
            return stream().filter(record -> record.get(column).equals(value)).collect(Collectors.toList());
        }
    }
}
