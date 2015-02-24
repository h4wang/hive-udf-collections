package com.hwang.hive.udf.array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDF;

public final class PatternConsolidate extends UDF{
	public static List<String> evaluate(ArrayList<String> array, int num) {
		if (array == null || array.size() == 0 || num == 0)
			return null;
		if(array.size() < num*2)
			{ 
			return array;}
		List<String> res = new ArrayList<String>(array);
		for(int i=0; i<= (res.size()-num*2); i++)
		{
				while(((i+2*num)<=res.size())&& (res.subList(i, i+num).equals(res.subList(i+num, i+2*num))))		
				{
					for(int j=0;j<=(num-1);j++){
						res.remove(i+num);}
				}					
		}
		
		return res;	
	}

}
