package com.hwang.hive.udf.array;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.ParseException;


public final class ArrayDateDiff extends UDF{
	public static List<Integer> evaluate(ArrayList<String> array) {
		if (array == null || array.size() < 2)
			return null;

		List<Integer> res = new ArrayList<Integer>();
		for(int i=1; i<array.size(); i++)
		{
			String cur_dt = array.get(i); 	
			String prev_dt = array.get(i-1);
			SimpleDateFormat dfm = new SimpleDateFormat("yyyyMMdd");
		    long diff = 0; 
				 try {
					diff = dfm.parse(cur_dt.toString()).getTime() - dfm.parse(prev_dt.toString()).getTime();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 int diffDays = (int) (diff / (24 * 60 * 60 * 1000));

		 res.add(diffDays);	
		}
		
		return res;	
	}

}
