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
public class SumarNumFichero
{
    public static void main( String[] args )
    {
    	//Step 1: 
    	SparkConf conf =new SparkConf();
    	
    	//Step 2:
    	
    	JavaSparkContext context = new JavaSparkContext(conf);
    	
    	/*
    	
    	JavaRDD<String> lines = context.textFile(args[0]);
    	
    	JavaRDD<Integer> rddOfIntegers = lines.map(s -> Integer.valueOf(s));
    	
    	int sum = rddOfIntegers.reduce( (integer1, integer2) -> (integer1+integer2));
    	*/
    	
    	int sum= context
    			.textFile(args[0])
    			.map(s -> Integer.valueOf(s))
    			.reduce( (integer1, integer2) -> (integer1+integer2));
    			
    			
    
    	
    	
        System.out.println( "Sum is: "+sum );
        
        context.stop();
    }
}