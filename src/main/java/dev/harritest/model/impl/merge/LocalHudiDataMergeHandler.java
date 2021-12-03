package dev.harritest.model.impl.merge;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import dev.harritest.controller.ContinentJSONSource;
import dev.harritest.controller.DataMergeHandler;
import dev.harritest.controller.HandleInstances;
import dev.harritest.model.AppConf;

public class LocalHudiDataMergeHandler implements DataMergeHandler{

	@Override
	public void mergeUpdateDelete() {

		SparkSession spark=SparkSession.builder().appName(AppConf.APP_NAME).master("local[*]")
				.getOrCreate();

		Logger logger=Logger.getLogger(LocalHudiDataMergeHandler.class.getName());
		
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
		
		
		/**
		 * END of get countries
		 */
		

		Dataset<Row> dataset = spark.read().option("header", true).csv(DetectMergeFile.lockone());
		
		dataset.createOrReplaceTempView("fifa_players");
		
		Dataset<Row> fifaplayers=spark.sql("select * from fifa_players");
		
		
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
		
		  final List<String> cnts=Arrays.asList("AF",
		  "AS",
		  "EU",
		  "NA",
		  "SA",
		  "OC",
		  "AN");
		  final Map<String, String> hudiOptions =new  Hashtable<>();
 		   //hudiOptions.put(DataSourceWriteOptions.TABLE_NAME().toString(), "fifa_players"+c);
			   hudiOptions.put(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(),"COPY_ON_WRITE");
		   hudiOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(),"player_name");
		   hudiOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "continent" );
		   hudiOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY() ,"last_update_time" );
//		   hudiOptions.put(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY() , "true"            );
//		   hudiOptions.put(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY() , "fifa_players"+c);
//		   hudiOptions.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY() , "creation_date");
//		   hudiOptions.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY() , MultiPartKeysValueExtractor.class.getName());
		


		  final Dataset<Row> dataresult=alljoin;
		  cnts.forEach((c) -> {
			  Dataset<Row> dspercontinent=dataresult.filter("continent='"+c+"'");
			  
			  /*ingest upsert*/
			  /*=====================================================================*/
			  String targetDest=AppConf.LOCAL_DEST_ANALYSIS_PATH+c+".csv";

 
			  	dspercontinent.write()
			   //.format("org.apache.hudi")
			   .format("hudi")
			   .options(hudiOptions)
			   .option("hoodie.table.name","fifa_players"+c)
			   .mode(SaveMode.Append)
			   .save(AppConf.LOCAL_MERGE_PATH);

			  /*=====================================================================*/
			  /*ingest upsert*/
				
			 
			  
		  });
		  
		  DetectMergeFile.processLocked();
		
	}
	

}
