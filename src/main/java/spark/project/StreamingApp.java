package spark.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingApp {
	
	final static Logger logger = LoggerFactory.getLogger(StreamingApp.class) ;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("streaming test").setMaster("yarn");
		logger.info(">> teste");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1l));
		streamingContext.fileStream("safsaf", GenericRecord.class, ParquetInputFormat<GenericRecord.class>);
		logger.info(">> print");
		streamingContext.stop();
		streamingContext.close();
	}

}















