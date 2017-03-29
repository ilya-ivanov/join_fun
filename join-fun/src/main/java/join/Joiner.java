package join;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Joiner {

    private final int leftColumnIndex;
    private final int rightColumnIndex;

    /**
     * @param leftJoinColumn column in left table to be used as primary key
     * @param rightJoinColumn column in right table to be used as foreign key
     */
    public Joiner(int leftJoinColumn, int rightJoinColumn) {
        this.leftColumnIndex = leftJoinColumn;
        this.rightColumnIndex = rightJoinColumn;
    }

    public RecordSet innerJoin(RecordSet leftTable, RecordSet rightTable) {
        int resultColumns = leftTable.columnsNum() + rightTable.columnsNum() - 1;
        RecordSet result = new RecordSet(resultColumns);

        leftTable.stream()
                .flatMap(leftRecord -> innerJoin(leftRecord, rightTable))
                .forEach(result::add);

        return result;
    }

    private Stream<List<String>> innerJoin(List<String> leftRecord, RecordSet rightTable) {
        return rightTable.stream()
                .filter(rightRecord -> rightRecord.get(rightColumnIndex).equals(leftRecord.get(leftColumnIndex)))
                .map(rightRecord -> merge(leftRecord, rightRecord));
    }

    private List<String> merge(List<String> leftRec, List<String> rightRec) {
        // do not include foreign key
        List<String> rightCopy = new ArrayList<>(rightRec);
        rightCopy.remove(rightColumnIndex);

        List<String> result = new ArrayList<>();
        result.addAll(leftRec);
        result.addAll(rightCopy);

        return result;
    }

}
