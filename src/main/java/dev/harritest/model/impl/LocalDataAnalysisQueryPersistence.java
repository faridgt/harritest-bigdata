package dev.harritest.model.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import dev.harritest.controller.DataAnalysisQueryPersistence;
import dev.harritest.model.AppConf;

public class LocalDataAnalysisQueryPersistence implements DataAnalysisQueryPersistence{

	@Override
	public void hitAnalysisAndPersist() {
		/**
		 * //1. Which top 3 countries that achieve the highest income through their players?
		 */
		
		SparkSession spark=SparkSession.builder().appName(AppConf.APP_NAME).master("local[*]")
				.getOrCreate();


		List<String> cnts=Arrays.asList("AF",
		  "AS",
		  "EU",
		  "NA",
		  "SA",
		  "OC",
		  "AN");


		int counter=0;
		Dataset<Row> maxds = null;
		for (String c:cnts) {
			  try {
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv(AppConf.LOCAL_DEST_INITIALIZE_PATH+c+".csv");
					dataset2.createOrReplaceTempView("fifa"+c);
					Dataset<Row> maxds2=spark.sql("select country_name,sum(salary_n) as total_value from fifa"+c+" group by country_name order by total_value desc limit 3 ");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(Exception e) {
					  System.out.println("Empty collection :-> "+ c);
				  }//

		}

		  maxds.createOrReplaceTempView("fifa_all");
		  maxds.show();
		  Dataset<Row> maxall=spark.sql("select country_name,total_value from fifa_all order by total_value desc limit 3 ");
		  maxall.show();
		  maxall.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_DEST_ANALYSIS_PATH+"countries_h_income.parquet");
	
		/**
		 * //2. List The club that includes the most valuable players, the top 5 clubs that spends highest salaries
		 */


		 counter=0;
		 maxds = null;
		for (String c:cnts) {
			  try {
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv(AppConf.LOCAL_DEST_INITIALIZE_PATH+c+".csv");
					dataset2.createOrReplaceTempView("fifa2"+c);
					Dataset<Row> maxds2=spark.sql("select Club,sum(salary_n) as total_value from fifa2"+c+" group by Club order by total_value desc limit 5 ");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(Exception e) {
					  System.out.println("Empty collection :-> "+ c);
				  }//

		}

		  maxds.createOrReplaceTempView("fifa_all_clubs");
		  maxds.show();
		  maxall=spark.sql("select Club,total_value from fifa_all_clubs order by total_value desc limit 5 ");
		  maxall.show();
		  maxall.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_DEST_ANALYSIS_PATH+"club_h_valuable_players.parquet");

		/**
		 * //3. Which of Europe or America - on continent level - has the best FIFA players?Note: Elaborate More about how your query is optimized to answer the questions above. Feel
          	free to structure the table/s as you see is better for the performance.

		 */

		 counter=0;
		 maxds = null;
		for (String c:cnts) {
			  try {
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv(AppConf.LOCAL_DEST_INITIALIZE_PATH+c+".csv");
					dataset2.createOrReplaceTempView("fifa3"+c);
					Dataset<Row> maxds2=spark.sql("select continent,sum(value_n) as total_value from fifa3"+c+" group by continent  ");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(Exception e) {
					  System.out.println("Empty collection :-> "+ c);
				  }//

		}

		  maxds.createOrReplaceTempView("fifa_all_continent");
		  maxds.show();
		  Dataset<Row> maxall_1=spark.sql("select continent,total_value from fifa_all_continent where continent ='EU' order by total_value desc limit 1 ");
		  Dataset<Row> maxall_2=spark.sql("select continent,sum(total_value) as total_value from fifa_all_continent where continent !='EU' group by continent  limit 1 ");
		  Dataset<Row> maxall_final=maxall_1.union(maxall_2);
		  
		  maxall_final.createOrReplaceTempView("fifa_all_continent_final");
		  maxall_final=spark.sql("select continent, total_value from fifa_all_continent_final order by total_value desc limit 1 ");
		  
		  maxall_final.show();
		  maxall.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_DEST_ANALYSIS_PATH+"/europe_or_America_best_players.parquet");
	
		
	
}

}
