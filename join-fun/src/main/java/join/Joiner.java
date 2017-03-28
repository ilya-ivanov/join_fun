package join;

import java.util.ArrayList;
import java.util.List;

public class Joiner {

    public static RecordSet innerJoin(RecordSet left, int leftJoinColumn, RecordSet right, int rightJoinColumn) {
        int resultColumns = left.columnsNum() + right.columnsNum() - 1;

        RecordSet result = new RecordSet(resultColumns);

        for (List<String> leftRecord : left) {
            String leftKey = leftRecord.get(leftJoinColumn);

            for (List<String> rightRecord : right) {
                String rightKey = rightRecord.get(rightJoinColumn);
                if (rightKey.equals(leftKey)) {
                    result.add(merge(leftRecord, rightRecord, rightJoinColumn));
                }
            }
        }

        return result;
    }

    private static List<String> merge(List<String> left, List<String> right, int rightJoinColumn) {
        // do not include foreign key
        List<String> rightCopy = new ArrayList<>(right);
        rightCopy.remove(rightJoinColumn);

        List<String> result = new ArrayList<>();
        result.addAll(left);
        result.addAll(rightCopy);

        return result;
    }

}
