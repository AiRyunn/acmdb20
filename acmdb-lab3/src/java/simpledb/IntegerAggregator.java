package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final boolean noGrouping;
    private String fieldname = null;
    private String gbfieldname = "";

    private final HashMap<Field, IntegerAggregateValue> groups;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.noGrouping = gbfield == Aggregator.NO_GROUPING;
        this.groups = new HashMap<Field, IntegerAggregateValue>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = this.noGrouping ? null : tup.getField(this.gbfield);
        IntegerAggregateValue aggValue = this.groups.get(key);
        int value = ((IntField) tup.getField(this.afield)).getValue();
        if (aggValue == null) {
            switch (this.what) {
                case COUNT:
                    aggValue = new CountValue(value);
                    break;
                case SUM:
                    aggValue = new SumValue(value);
                    break;
                case MIN:
                    aggValue = new MinValue(value);
                    break;
                case MAX:
                    aggValue = new MaxValue(value);
                    break;
                case AVG:
                    aggValue = new AvgValue(value);
                    this.groups.put(key, new AvgValue(value));
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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

    private abstract class IntegerAggregateValue {
        abstract void put(Integer value);

        abstract Field getValue();
    }

    private class CountValue extends IntegerAggregateValue {
        private int count;

        CountValue(Integer value) {
            this.count = 1;
        }

        @Override
        void put(Integer value) {
            this.count++;
        }

        @Override
        IntField getValue() {
            return new IntField(this.count);
        }

    }

    private class SumValue extends IntegerAggregateValue {
        private int sum;

        SumValue(Integer value) {
            this.sum = value;
        }

        @Override
        void put(Integer value) {
            this.sum += value;
        }

        @Override
        IntField getValue() {
            return new IntField(this.sum);
        }

    }

    private class MinValue extends IntegerAggregateValue {
        private int min;

        MinValue(Integer value) {
            this.min = value;
        }

        @Override
        void put(Integer value) {
            if (value < this.min) {
                this.min = value;
            }
        }

        @Override
        IntField getValue() {
            return new IntField(this.min);
        }

    }

    private class MaxValue extends IntegerAggregateValue {
        private int max;

        MaxValue(Integer value) {
            this.max = value;
        }

        @Override
        void put(Integer value) {
            if (value > this.max) {
                this.max = value;
            }
        }

        @Override
        IntField getValue() {
            return new IntField(this.max);
        }

    }

    private class AvgValue extends IntegerAggregateValue {
        private int count, sum;

        AvgValue(Integer value) {
            this.count = 1;
            this.sum = value;
        }

        @Override
        void put(Integer value) {
            this.count++;
            this.sum += value;
        }

        @Override
        IntField getValue() {
            return new IntField(this.sum / this.count);
        }

    }
}
