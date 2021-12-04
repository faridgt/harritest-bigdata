package dev.harritest.model.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import dev.harritest.controller.DataAnalysisPrepareTable;
import dev.harritest.model.AppConf;

public class CloudDataAnalysisPrepareTable implements DataAnalysisPrepareTable{

		@Override
		public void prepareDataTable() {

		
		SparkSession spark=SparkSession.builder().appName(AppConf.APP_NAME)
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
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv(AppConf.CLOUD_DEST_INITIALIZE_PATH+c+".csv");
					dataset2.createOrReplaceTempView("fifa"+c);
					Dataset<Row> maxds2=spark.sql("select continent, country_name, Club ,sum(value_n) as total_value ,sum(salary_n) as total_salary from fifa"+c+" group by continent, country_name, Club");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(Exception e) {
					  System.out.println("Empty collection :-> "+ c);
				  }//

		}

		 
		  maxds.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.CLOUD_DEST_PREPARE_ANALYSIS_PATH+"prepare_data_table.parquet");

		
	}
	}

