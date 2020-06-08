package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final boolean noGrouping;
    private String fieldname = null;
    private String gbfieldname = "";

    private final HashMap<Field, StringAggregateValue> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.noGrouping = gbfield == Aggregator.NO_GROUPING;
        groups = new HashMap<Field, StringAggregateValue>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = this.noGrouping ? null : tup.getField(this.gbfield);
        StringAggregateValue aggValue = this.groups.get(key);
        String value = ((StringField) tup.getField(this.afield)).getValue();
        if (aggValue == null) {
            switch (this.what) {
                case COUNT:
                    aggValue = new CountValue(value);
                    break;
                default:
                    assert false;
            }
            this.groups.put(key, aggValue);
        } else {
            aggValue.put(value);
        }

        // NOTE: get names when the first tuple is merged into group
        if (this.fieldname == null) {
            this.fieldname = tup.getTupleDesc().getFieldName(this.afield);
            if (!noGrouping) {
                this.gbfieldname = tup.getTupleDesc().getFieldName(this.gbfield);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc tupledesc = this.getTupleDesc();
        for (Field key : this.groups.keySet()) {
            Field aggValue = groups.get(key).getValue();
            Tuple t = new Tuple(tupledesc);

            if (this.noGrouping) {
                t.setField(0, aggValue);
            } else {
                t.setField(0, key);
                t.setField(1, aggValue);
            }

            tuples.add(t);
        }
        return new TupleIterator(tupledesc, tuples);
    }

    public TupleDesc getTupleDesc() {
        String[] names;
        Type[] types;
        if (this.noGrouping) {
            names = new String[] { this.fieldname };
            types = new Type[] { Type.INT_TYPE };
        } else {
            names = new String[] { this.gbfieldname, this.fieldname };
            types = new Type[] { this.gbfieldtype, Type.INT_TYPE };
        }
        return new TupleDesc(types, names);
    }

    private abstract class StringAggregateValue {
        abstract void put(String value);

        abstract Field getValue();
    }

    private class CountValue extends StringAggregateValue {
        private int count;

        CountValue(String value) {
            this.count = 1;
        }

        @Override
        void put(String value) {
            this.count++;
        }

        @Override
        IntField getValue() {
            return new IntField(this.count);
        }

    }

}
