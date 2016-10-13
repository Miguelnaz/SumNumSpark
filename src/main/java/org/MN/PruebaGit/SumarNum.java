package org.MN.PruebaGit;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class SumarNum 
{
    public static void main( String[] args )
    {
    	//Step 1: 
    	SparkConf conf =new SparkConf();
    	
    	//Step 2:
    	
    	JavaSparkContext context = new JavaSparkContext(conf);
    	
    	//Step3:
    	
    	Integer[] numbers = new Integer[] {3,56,364,32,236524757,7750};
    	List<Integer>listOfNumbers= Arrays.asList(numbers);
    	
    	JavaRDD<Integer> rddOfNumbers = context.parallelize(listOfNumbers);
    	
    	int sum = rddOfNumbers.reduce( (integer1, integer2) -> (integer1+integer2));
    	
    	
        System.out.println( "Sum is: "+sum );
    }
}
