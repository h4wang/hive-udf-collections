package com.hwang.hive.udf.array;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;


/**
 * Based on java.util.Arrays.copyOfRange
 *
 */

public class ArrayRange extends GenericUDF {
        private ListObjectInspector listInspector;
        private StandardListObjectInspector returnInspector;
        private IntObjectInspector firstIntInspector, secondIntInspector;
        
        public int[] getIndexes(DeferredObject[] arg0) throws HiveException {
            int start, end, length;
            Object list =  arg0[0].get();
            if (secondIntInspector == null) {
                    start = 0;
                    end = firstIntInspector.get( arg0[1].get());
                    length = end;
            } else {
                    start = firstIntInspector.get( arg0[1].get());
                    if (secondIntInspector.get( arg0[2].get())<0)
                    { 
                    	end = listInspector.getListLength( list ) + secondIntInspector.get( arg0[2].get());
                    }
                    else
                    end = secondIntInspector.get( arg0[2].get());
                    
                    length = end-start+1;
            }

            return new int[] {start, end, length};
        }
        
        @Override
        public Object evaluate(DeferredObject[] arg0) throws HiveException {

                int indexes[] = getIndexes(arg0);

                int start = indexes[0];
                int end = indexes[1];
                int length = indexes[2];

                Object uninspListObj = arg0[0].get();
                
                int listSize = listInspector.getListLength(uninspListObj);
                
                Object truncatedListObj =  returnInspector.create(length);

        for(int i=0; i< end && i <listSize; ++i) {
                returnInspector.set( truncatedListObj, i,  listInspector.getListElement( uninspListObj, i+start ));
        }
        return truncatedListObj;
        }


        @Override
        public String getDisplayString(String[] arg0) {
                return "array_range(" + arg0[0] + ", " + arg0[1] + " , " + arg0[1] + " )";
        }


        @Override
        public ObjectInspector initialize(ObjectInspector[] arg0)
                        throws UDFArgumentException {
                ObjectInspector first = arg0[0];
                if(first.getCategory() == Category.LIST) {
                        listInspector = (ListObjectInspector) first;
                } else {
                        throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
                }
                
            ObjectInspector second = arg0[1];
            if( second.getCategory() == Category.PRIMITIVE) {
                    PrimitiveObjectInspector secondPrim = (PrimitiveObjectInspector) second;
                    if( secondPrim.getPrimitiveCategory() == PrimitiveCategory.INT ) {
                            firstIntInspector= (IntObjectInspector) second;
                    } else {
                          throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
                    }
            } else {
                        throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
            }

            if (arg0.length > 2) {

                           ObjectInspector third = arg0[2];
                    if ( third != null ) {
                            if( third.getCategory() == Category.PRIMITIVE) {
                                PrimitiveObjectInspector thirdPrim = (PrimitiveObjectInspector) third;
                                if( thirdPrim.getPrimitiveCategory() == PrimitiveCategory.INT ) {
                                        secondIntInspector= (IntObjectInspector) third;
                                } else {
                                      throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
                                }
                            } else {
                                    throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
                            }
                    }
            }
            
            
            returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                            listInspector.getListElementObjectInspector());
                return returnInspector;
        }

}
