package dev.harritest.model.impl;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import dev.harritest.controller.DataAnalysisQueryPersistence;
import dev.harritest.model.AppConf;

public class CloudDataAnalysisQueryPersistence  implements DataAnalysisQueryPersistence {



		@Override
		public void hitAnalysisAndPersist() {
			/**
			 * //1. Which top 3 countries that achieve the highest income through their players?
			 */
			
			SparkSession spark=SparkSession.builder().appName(AppConf.APP_NAME)
					.getOrCreate();



			

			  Dataset<Row> dataset2 = spark.read().option("header", true).parquet(AppConf.CLOUD_DEST_PREPARE_ANALYSIS_PATH+"prepare_data_table.parquet");
	          dataset2.createOrReplaceTempView("fifa_prepare");

			  Dataset<Row> maxall=spark.sql("select country_name,sum(total_salary) as total_value  from fifa_prepare group by country_name order by total_value desc limit 3 ");
			  maxall.show();
			  maxall.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.CLOUD_DEST_ANALYSIS_PATH+"countries_h_income.parquet");
		
			/**
			 * //2. List The club that includes the most valuable players, the top 5 clubs that spends highest salaries
			 */


			  
			  
			
			  Dataset<Row> maxds2=spark.sql("select Club,sum(total_salary) as total_value from fifa_prepare group by Club order by total_value desc limit 5 ");
						
						 
			  maxds2.show();

			  maxds2.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.CLOUD_DEST_ANALYSIS_PATH+"club_h_valuable_players.parquet");

			/**
			 * //3. Which of Europe or America - on continent level - has the best FIFA players?Note: Elaborate More about how your query is optimized to answer the questions above. Feel
	          	free to structure the table/s as you see is better for the performance.

			 */
			  //select continent, country_name, Club ,sum(value_n) as total_value ,sum(salary_n) as total_salary
			  Dataset<Row> maxds3_1=spark.sql("select 'America' as continent,sum(total_value) as total_value from fifa_prepare where continent in ('SA','NA')");
	          maxds3_1.createOrReplaceTempView("p3_t1");
	          Dataset<Row> maxds3_2=spark.sql("select 'Europe' as continent,sum(total_value) as total_value from fifa_prepare where continent in ('EU')");
	          maxds3_2=maxds3_1.union(maxds3_2);
	          maxds3_2.createOrReplaceTempView("p3_t2");
	          Dataset<Row> maxds33=spark.sql("select continent, total_value from p3_t2 order by total_value desc limit 1");  
	          
	          maxds33.show();

	          maxds33.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.CLOUD_DEST_ANALYSIS_PATH+"/europe_or_America_best_players.parquet");
		
			
		
	}

	}
	