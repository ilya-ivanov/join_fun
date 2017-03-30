package join;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

public class Record extends ArrayList<String> implements Comparable<Record> {

    public Record(String... values) {
        this.addAll(asList(values));
    }

    public Record(List<String> values) {
        this.addAll(values);
    }

    @Override
    public int compareTo(Record that) {
        if (that == null) {
            return -1;
        }

        if (this.size() != that.size()) {
            return this.size() - that.size();
        }

        for (int i = 0; i < this.size(); i++) {
            int fieldCompareResult = this.get(i).compareTo(that.get(i));

            if (fieldCompareResult != 0) {
                return fieldCompareResult;
            }
        }

        return 0;
    }

}
