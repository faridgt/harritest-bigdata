package dev.harritest.model.impl;


	import static org.apache.spark.sql.functions.callUDF;

	import static org.apache.spark.sql.functions.col;
	import java.io.IOException;
	import java.util.Arrays;
	import java.util.List;
	import org.apache.log4j.Logger;
	import org.apache.spark.sql.Dataset;
	import org.apache.spark.sql.Encoders;
	import org.apache.spark.sql.Row;
	import org.apache.spark.sql.SparkSession;
	import org.apache.spark.sql.api.java.UDF1;
	import org.apache.spark.sql.types.DataTypes;
	import dev.harritest.controller.ContinentJSONSource;
	import dev.harritest.controller.DataHandlerInitializer;
	import dev.harritest.controller.HandleInstances;
	import dev.harritest.model.AppConf;
	
	/**
	 * 
	 * @author farid
	 *This is class provide cloud implementation for data initialaization  
	 */

	public class CloudDataHandlerInitializer implements DataHandlerInitializer{

		
	@Override
	public void handleInitial() {
		Logger logger=Logger.getLogger(LocalDataHandlerInitializer.class.getName());
		
		SparkSession spark=SparkSession.builder().appName(AppConf.APP_NAME)
				.getOrCreate();

		
		/**
		 * get countries
		 */
		String jsonStr="";
		 ContinentJSONSource continentJSONSourceInst=HandleInstances.getOrCreateContinentJSONSourceInstance();
		 try {
		 jsonStr=continentJSONSourceInst.getJSONData();
		 } catch (IOException e) {
			logger.error("Unable to get the contininets json data"); 
			return;
		}
		

		
		List<String> jsonData = Arrays.asList(jsonStr);
		

		Dataset<String> baseds = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> countriesds = spark.read().json(baseds.toJavaRDD());
		countriesds.createOrReplaceTempView("countries");
		
		countriesds.show();
		/**
		 * END of get countries
		 */
		
		
		Dataset<Row> dataset = spark.read().option("header", true).csv(AppConf.CLOUD_SOURCE_PATH);
		
		dataset.createOrReplaceTempView("fifa_players");
		
		Dataset<Row> fifaplayers=spark.sql("select * from fifa_players");
		
		fifaplayers.show();
		
		
		UDF1<String,String> rep = new UDF1<String,String>() 
	    {
	        private static final long serialVersionUID = -5239951370238629896L;

	        @Override
	        public String call(String t1) throws Exception {
	            return t1.replace("€","").replace("K","000").replace("M","000000");
	        }
	    };
	    spark.udf().register("rep", rep, DataTypes.StringType);
	    
	    
		
		Dataset<Row> alljoin=spark.sql("select  a.countryContinent as continent , a.code as country_code,a.name as country_name, b.name as player_name,b.age,\"Fifa Score\" as fifa_score,Club ,Value, Salary   from countries  a inner join fifa_players b  on a.name=b.Nationality");

		
		alljoin=alljoin.select("country_name", "player_name", "country_code", "age", "continent", "Value", "fifa_score", "Club",  "Salary")
	    .withColumn("salary_n", callUDF("rep", col("Salary")))
	    .withColumn("value_n", callUDF("rep", col("Value")));
		
		  List<String> cnts=Arrays.asList("AF",
		  "AS",
		  "EU",
		  "NA",
		  "SA",
		  "OC",
		  "AN");
		  final Dataset<Row> dataresult=alljoin;
		  cnts.forEach((c) -> {
			  Dataset<Row> dspercontinent=dataresult.filter("continent='"+c+"'");
			  dspercontinent.write().option("header", true).csv(AppConf.CLOUD_DEST_INITIALIZE_PATH +c+".csv");
			  
		  });
		  
	    
		
		
		
		
		
		
		


	}


	}
