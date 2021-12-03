package dev.harritest.model.impl.merge;

import dev.harritest.controller.DataMergeHandler;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import dev.harritest.controller.ContinentJSONSource;
import dev.harritest.controller.HandleInstances;
import dev.harritest.model.AppConf;

/**
 * 
 * @author farid
 * @implNote This is called ugly solution since it does not generate clean tables so can just normally apply query info on them
 * but instead it use a staging area to create new files for the updates only 
 * the query will just join the data and check for similar info and just choose them for the new staging part
 * deletion can utilize similar solution by write to a delete stage and only key fields to be written like player_name and continent 
 * 
 * The ugly part is on the query generation as now we need to select from base plus stages and filter older information by choosing the new side information
 * as select a.* where a.id=b.id 
 * The more ugly part is to keep up with stages over time as we need to join more files and we to choose the newest 
 * 
 * The actual solution obviously will need long time to be completed but the gain from the performance side result is much better than 
 * full/actual merge and update  
 * 
 *  There is a solution called hudi that implement a beautiful solution with checkpoint default upsert timestamps and time keys and history
 *  I almost got it working locally there some dependencies that I need to fix
 *  On Amazon EMR it has same issue on the dependency even though it is included in the clusters 
 *  
 *  need a little more digging  
 */
public class LocalUglyDataMergeHandler implements DataMergeHandler {


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


		  final Dataset<Row> dataresult=alljoin;
		  cnts.forEach((c) -> {
			  try {

			  Dataset<Row> dsnew=dataresult.filter("continent='"+c+"'");
			  dsnew.createOrReplaceTempView("fifa_new_"+c);
			  
			  Dataset<Row> dataset_original = spark.read().option("header", true).csv(AppConf.LOCAL_DEST_INITIALIZE_PATH+c+".csv");
			  dataset_original.createOrReplaceTempView("fifa"+c);
			  /**
			   * assuming update on salary field, to be generalized on all other fields 
			   */
			  Dataset<Row> updatedRecords=spark.sql("select a.* from fifa_new_"+c+" a inner join fifa"+c+" b on a.player_name=b.player_name and a.salary_v!=b.salary_name");
			  
			  updatedRecords.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_MERGE_PATH+"c.parquet");

			  }catch(Exception e) {
				  System.out.println("Empty collection :-> "+ c);
			  }//
				
			 
			  
		  });
		  
		  DetectMergeFile.processLocked();
		
	}
	

}
