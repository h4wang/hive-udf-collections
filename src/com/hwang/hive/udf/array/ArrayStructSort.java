package com.hwang.hive.udf.array;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;

import java.util.ArrayList;
import java.util.Collections;



public class ArrayStructSort extends GenericUDF {

    private ListObjectInspector loi;    // nkelkar --comment an Object Inspector for a LIST type of element

    private StructObjectInspector structoi; // nkelkar --comment an Object Inspector for a STRUCT type of element

    private ArrayList<Object []> ret;

    private IntObjectInspector intoi = null;  // nkelkar --comment an Object Inspector for a PRIMITIVE type of element
    
    private ObjectInspector ssoi, stoi;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        // nkelkar --comment first, determine how many input arguments are provided. Based on various argument
        //                   lengths, decide how to respond in each case
        switch (args.length) {
            case 1:
                break;
            case 2: // nkelkar --comment in this case, the user also provided a flag to indicate the first k elements
                    //                   that he/she wants
                if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {  // nkelkar --comment check whether the second argument is PRIMITIVE in type
                    throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only a primitive type" +
                            " of data for the second argument, if provided");
                }

                if (((PrimitiveObjectInspector)args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {   // nkelkar --comment if primitive, check whether
                    throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only an int type" +                //                   its an INT type
                            " of data for the second argument, if provided. " + args[1].getTypeName() + " found for the second argument instead");
                }
                intoi = (IntObjectInspector)args[1]; // nkelkar --comment store an Object Inspector for this argument
                break;
            default:
                throw new UDFArgumentLengthException("The function sortStructArray() takes in either one or two arguments");
        }

        // nkelkar --comment since the first argument will always be there, this is
        //                   processed outside of the switch-case statement
        if (args[0].getCategory() != ObjectInspector.Category.LIST) {  // nkelkar --comment check whether the first argument is of LIST type
            throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only a list type" +
                    " of data for the first argument");
        }


        if (((ListObjectInspector)args[0]).getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT) { // nkelkar --comment within his LIST, check whether
            throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only a list<struct> type" +         //                   there are STRUCT elements
                    " of data for the first argument. " + "However, list<" + ((ListObjectInspector)args[0]).getListElementObjectInspector().getCategory() + "> was found instead");
        }

        loi = (ListObjectInspector)args[0];

        structoi = (StructObjectInspector)loi.getListElementObjectInspector();

        // nkelkar --comment get handles for the fields within the struct
        StructField slice = structoi.getStructFieldRef("slice");
        StructField timestamp = structoi.getStructFieldRef("timestamp");
        
        ssoi = slice.getFieldObjectInspector();
        stoi = timestamp.getFieldObjectInspector();
        

        // nkelkar --comment are all the field names the same as we expect them to be?
        if (slice == null)
            throw new UDFArgumentTypeException(0,"No \"slice\" field in input structure "+structoi.getTypeName());
        if (timestamp == null)
            throw new UDFArgumentTypeException(0,"No \"timestamp\" field in input structure "+structoi.getTypeName());


        if (ssoi.getCategory() != ObjectInspector.Category.PRIMITIVE || stoi.getCategory() != ObjectInspector.Category.PRIMITIVE) { // nkelkar --comment check whether both
            throw new UDFArgumentTypeException(0, "Struct elements passed in arg[0] were not found to be of STRUCT<PRIMITIVE, PRIMITIVE> type"); //                fields within the struct are PRIMITIVE type
        }

        if (((PrimitiveObjectInspector)ssoi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG ||  // nkelkar --comment check whether the first struct elem
            ((PrimitiveObjectInspector)ssoi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {     //                   is a STRING, and the second an INT
            throw new UDFArgumentTypeException(0, "Struct elements passed in arg[0] were not found to be of STRUCT<BIGINT, BIGINT> type");
        }


        ret = new ArrayList<Object []>(); // nkelkar --comment initialize the variable to return

        ArrayList<String> structFieldNames = new ArrayList<String>();
        structFieldNames.add("slice");      // nkelkar --comment pick the names that we want to give to our output struct fields
        structFieldNames.add("timestamp");

        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector); // nkelkar --comment prepare struct field Object Inspectors

        // nkelkar --comment return an object inspector that reflects the skeletal structure of the actual object "ret" that would be returned
        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors));
    }
    
    public class UserVisitsDataStruct implements Comparable<UserVisitsDataStruct> {

        private long slice;
        private long timestamp;

        public UserVisitsDataStruct(long slice, long timestamp) {
            this.slice = slice;
            this.timestamp = timestamp;
        }

        public UserVisitsDataStruct() {}

        public long getUserId() {
            return this.slice;
        }

        public long getNumVisits() {
            return this.timestamp;
        }

        @Override
        public int compareTo(UserVisitsDataStruct o) {
            return (this.slice == o.slice)?0:((this.slice < o.slice)?-1:1);
        }
    }

    @Override
    public ArrayList<Object []> evaluate(DeferredObject[] arguments) throws HiveException {
        ret.clear();  // nkelkar --comment clear the ret array from objects from previous calls
        int top_k = -1; // nkelkar --comment switch to indicate number of elements to be returned

        switch (arguments.length) {
            case 1:
                if (arguments[0] == null) return null; // nkelkar --comment check for nullity in argument(s)
                break;
            case 2:
                if (arguments[0] == null || arguments[1] == null) return null; // nkelkar --comment check for nullity in argument(s)
                top_k = intoi.get(arguments[1].get()); // nkelkar --comment get the number of interested elements
            default:
                return null;
        }

        if (top_k < 0 || top_k > loi.getListLength(arguments[0].get())) // nkelkar --comment this helps ignore the second argument
            top_k = loi.getListLength(arguments[0].get());              //                   in the case that the number of elements
                                                                        //                   to be returned as per the user request > length of array

        // nkelkar --comment first, read in the data
        ArrayList<UserVisitsDataStruct> intermediate_arr = new ArrayList<UserVisitsDataStruct>();

        for (int i=0; i<loi.getListLength(arguments[0].get()); i++) {
            // nkelkar --comment store the data into temporary variables
        	LongWritable temp_slice = (LongWritable)structoi.getStructFieldData(loi.getListElement(arguments[0].get(), i), structoi.getStructFieldRef("slice"));
        	long tt = ((LongObjectInspector)ssoi).get( temp_slice );
        	
        	LongWritable temp_timestamp = (LongWritable)structoi.getStructFieldData(loi.getListElement(arguments[0].get(), i), structoi.getStructFieldRef("timestamp"));
        	long qq = ((LongObjectInspector)stoi).get( temp_timestamp );
        	
            // nkelkar --comment package the data into our custom struct org.nkelkar.utils.UserVisitsDataStruct
            //                   and add it to the intermediate array
//        	Object[] e;
//            e = new Object[2];
//
//            e[0] = new LongWritable(tt);
//            e[1] = new LongWritable(qq);
            intermediate_arr.add(new UserVisitsDataStruct(tt, qq));

        }

        // nkelkar --comment reverse sort the intermediate array based on the number of visits by each slice
        Collections.sort(intermediate_arr);

        // nkelkar --comment pick out the top k elements from the intermediate array, pack them into
        //                   the arraylist to be returned, and send!
        for (int i=0; i<top_k; i++) {
            Object [] ret_obj = new Object [2];
            ret_obj[0] = new LongWritable(intermediate_arr.get(i).getUserId());
            ret_obj[1] = new LongWritable(intermediate_arr.get(i).getNumVisits());
            ret.add(ret_obj); // nkelkar --comment add the Object [] to the array to send back to Hive
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert(strings.length < 3); // nkelkar --comment assert that only a maqximum of 2 arguments can be provided

        if (strings.length == 1) // nkelkar --comment if only one argument was provided, print this
            return "sortStructArray(" + strings[0] + ")";
        else // nkelkar --comment else, print this
            return "sortStructArray(" + strings[0] + "," + strings[1] + ")";
    }
}


