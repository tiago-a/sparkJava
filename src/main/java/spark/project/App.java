package spark.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Logger.getLogger("org").setLevel(Level.WARN);
    	List<Double> inputData = new ArrayList<>();
    	inputData.add(32.1);
    	inputData.add(54.3);
    	inputData.add(17.7);
        SparkSession sparkSession = SparkSession.builder().appName("App").master("local").getOrCreate();
        
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        
        JavaRDD<Double> parallelize = jsc.parallelize(inputData);
        
        System.out.println("->> " + parallelize.take(3));
        
        parallelize.foreach(new VoidFunction<Double>() {
			
			@Override
			public void call(Double t) throws Exception {
				System.out.println("-> " + t);				
			}
		});
        
        
        System.out.println("->> " + sparkSession.toString());
        
        
        jsc.close();
        
    }
}
