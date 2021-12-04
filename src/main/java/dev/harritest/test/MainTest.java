package dev.harritest.test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

import dev.harritest.model.AppConf;


 
public class MainTest {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
//		System.setProperty("hadoop.home.dir", "/opt/hadoop_old");
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("o").setLevel(Level.WARN);
		Logger.getLogger("com").setLevel(Level.WARN);

		Logger.getRootLogger().setLevel(Level.WARN);
		
		test14_prepared_q();
		
		
	}

	private static void test14_prepared_q() {

		SparkSession spark=SparkSession.builder().appName(AppConf.APP_NAME).master("local[*]")
				.getOrCreate();



		

		  Dataset<Row> dataset2 = spark.read().option("header", true).parquet(AppConf.LOCAL_DEST_PREPARE_ANALYSIS_PATH+"prepare_data_table.parquet");
          dataset2.createOrReplaceTempView("fifa_prepare");

		  Dataset<Row> maxall=spark.sql("select country_name,sum(total_salary) as total_value  from fifa_prepare group by country_name order by total_value desc limit 3 ");
		  maxall.show();
		  maxall.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_DEST_ANALYSIS_PATH+"countries_h_income.parquet");
	
		/**
		 * //2. List The club that includes the most valuable players, the top 5 clubs that spends highest salaries
		 */


		  
		  
		
		  Dataset<Row> maxds2=spark.sql("select Club,sum(total_salary) as total_value from fifa_prepare group by Club order by total_value desc limit 5 ");
					
					 
		  maxds2.show();

		  maxds2.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_DEST_ANALYSIS_PATH+"club_h_valuable_players.parquet");

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

          maxds33.write().option("header", true).mode(SaveMode.Overwrite).parquet(AppConf.LOCAL_DEST_ANALYSIS_PATH+"/europe_or_America_best_players.parquet");
		
		

	}	
	
	private static void test1() {
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataset.show();
		long numOfRow=dataset.count();
		System.out.println("There are "+ numOfRow+ " records");

		Row firstRow=dataset.first();
		//String subject=firstRow.get(2).toString();
		String subject=firstRow.getAs("subject").toString();
		System.out.println(subject);
		
		int year=Integer.parseInt(firstRow.getAs("year").toString());
		System.out.println(year);

		
	}

	
	private static void test2_filter() {
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		Dataset<Row> moderArt=dataset.filter("subject = 'Modern Art' and year>=2007 ");
		moderArt.show();

		
	}



	private static void test3_sql() {
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset.createOrReplaceTempView("students");
		
		Dataset<Row> moderArt=spark.sql("select * from students where subject = 'Modern Art' and year>=2007 ");
		
		moderArt.show();

		
	}

	private static void test4_sql_max() {
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset.createOrReplaceTempView("students");
		
		Dataset<Row> maxds=spark.sql("select year,max(score) as top from students group by year order by top  desc");
		
		maxds.show();

		
	}


	private static void test5_json() {
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();
		
		
		String jsonStr="";////		"[{\"code\":\"AF\",\"name\":\"Afghanistan\",\"fullName\":\"Islamic Republic of Afghanistan\",\"iso3\":\"AFG\",\"number\":\"004\",\"countryContinent\":\"AS\"},{\"code\":\"AX\",\"name\":\"Åland Islands\",\"fullName\":\"Åland Islands\",\"iso3\":\"ALA\",\"number\":\"248\",\"countryContinent\":\"EU\"},{\"code\":\"AL\",\"name\":\"Albania\",\"fullName\":\"Republic of Albania\",\"iso3\":\"ALB\",\"number\":\"008\",\"countryContinent\":\"EU\"},{\"code\":\"DZ\",\"name\":\"Algeria\",\"fullName\":\"People's Democratic Republic of Algeria\",\"iso3\":\"DZA\",\"number\":\"012\",\"countryContinent\":\"AF\"},{\"code\":\"AS\",\"name\":\"American Samoa\",\"fullName\":\"American Samoa\",\"iso3\":\"ASM\",\"number\":\"016\",\"countryContinent\":\"OC\"},{\"code\":\"AD\",\"name\":\"Andorra\",\"fullName\":\"Principality of Andorra\",\"iso3\":\"AND\",\"number\":\"020\",\"countryContinent\":\"EU\"},{\"code\":\"AO\",\"name\":\"Angola\",\"fullName\":\"Republic of Angola\",\"iso3\":\"AGO\",\"number\":\"024\",\"countryContinent\":\"AF\"},{\"code\":\"AI\",\"name\":\"Anguilla\",\"fullName\":\"Anguilla\",\"iso3\":\"AIA\",\"number\":\"660\",\"countryContinent\":\"NA\"},{\"code\":\"AQ\",\"name\":\"Antarctica\",\"fullName\":\"Antarctica (the territory South of 60 deg S)\",\"iso3\":\"ATA\",\"number\":\"010\",\"countryContinent\":\"AN\"},{\"code\":\"AG\",\"name\":\"Antigua and Barbuda\",\"fullName\":\"Antigua and Barbuda\",\"iso3\":\"ATG\",\"number\":\"028\",\"countryContinent\":\"NA\"},{\"code\":\"AR\",\"name\":\"Argentina\",\"fullName\":\"Argentine Republic\",\"iso3\":\"ARG\",\"number\":\"032\",\"countryContinent\":\"SA\"},{\"code\":\"AM\",\"name\":\"Armenia\",\"fullName\":\"Republic of Armenia\",\"iso3\":\"ARM\",\"number\":\"051\",\"countryContinent\":\"AS\"},{\"code\":\"AW\",\"name\":\"Aruba\",\"fullName\":\"Aruba\",\"iso3\":\"ABW\",\"number\":\"533\",\"countryContinent\":\"NA\"},{\"code\":\"AU\",\"name\":\"Australia\",\"fullName\":\"Commonwealth of Australia\",\"iso3\":\"AUS\",\"number\":\"036\",\"countryContinent\":\"OC\"},{\"code\":\"AT\",\"name\":\"Austria\",\"fullName\":\"Republic of Austria\",\"iso3\":\"AUT\",\"number\":\"040\",\"countryContinent\":\"EU\"},{\"code\":\"AZ\",\"name\":\"Azerbaijan\",\"fullName\":\"Republic of Azerbaijan\",\"iso3\":\"AZE\",\"number\":\"031\",\"countryContinent\":\"AS\"},{\"code\":\"BS\",\"name\":\"Bahamas\",\"fullName\":\"Commonwealth of the Bahamas\",\"iso3\":\"BHS\",\"number\":\"044\",\"countryContinent\":\"NA\"},{\"code\":\"BH\",\"name\":\"Bahrain\",\"fullName\":\"Kingdom of Bahrain\",\"iso3\":\"BHR\",\"number\":\"048\",\"countryContinent\":\"AS\"},{\"code\":\"BD\",\"name\":\"Bangladesh\",\"fullName\":\"People's Republic of Bangladesh\",\"iso3\":\"BGD\",\"number\":\"050\",\"countryContinent\":\"AS\"},{\"code\":\"BB\",\"name\":\"Barbados\",\"fullName\":\"Barbados\",\"iso3\":\"BRB\",\"number\":\"052\",\"countryContinent\":\"NA\"},{\"code\":\"BY\",\"name\":\"Belarus\",\"fullName\":\"Republic of Belarus\",\"iso3\":\"BLR\",\"number\":\"112\",\"countryContinent\":\"EU\"},{\"code\":\"BE\",\"name\":\"Belgium\",\"fullName\":\"Kingdom of Belgium\",\"iso3\":\"BEL\",\"number\":\"056\",\"countryContinent\":\"EU\"},{\"code\":\"BZ\",\"name\":\"Belize\",\"fullName\":\"Belize\",\"iso3\":\"BLZ\",\"number\":\"084\",\"countryContinent\":\"NA\"},{\"code\":\"BJ\",\"name\":\"Benin\",\"fullName\":\"Republic of Benin\",\"iso3\":\"BEN\",\"number\":\"204\",\"countryContinent\":\"AF\"},{\"code\":\"BM\",\"name\":\"Bermuda\",\"fullName\":\"Bermuda\",\"iso3\":\"BMU\",\"number\":\"060\",\"countryContinent\":\"NA\"},{\"code\":\"BT\",\"name\":\"Bhutan\",\"fullName\":\"Kingdom of Bhutan\",\"iso3\":\"BTN\",\"number\":\"064\",\"countryContinent\":\"AS\"},{\"code\":\"BO\",\"name\":\"Bolivia\",\"fullName\":\"Plurinational State of Bolivia\",\"iso3\":\"BOL\",\"number\":\"068\",\"countryContinent\":\"SA\"},{\"code\":\"BQ\",\"name\":\"Bonaire, Sint Eustatius and Saba\",\"fullName\":\"Bonaire, Sint Eustatius and Saba\",\"iso3\":\"BES\",\"number\":\"535\",\"countryContinent\":\"NA\"},{\"code\":\"BA\",\"name\":\"Bosnia and Herzegovina\",\"fullName\":\"Bosnia and Herzegovina\",\"iso3\":\"BIH\",\"number\":\"070\",\"countryContinent\":\"EU\"},{\"code\":\"BW\",\"name\":\"Botswana\",\"fullName\":\"Republic of Botswana\",\"iso3\":\"BWA\",\"number\":\"072\",\"countryContinent\":\"AF\"},{\"code\":\"BV\",\"name\":\"Bouvet Island (Bouvetoya)\",\"fullName\":\"Bouvet Island (Bouvetoya)\",\"iso3\":\"BVT\",\"number\":\"074\",\"countryContinent\":\"AN\"},{\"code\":\"BR\",\"name\":\"Brazil\",\"fullName\":\"Federative Republic of Brazil\",\"iso3\":\"BRA\",\"number\":\"076\",\"countryContinent\":\"SA\"},{\"code\":\"IO\",\"name\":\"British Indian Ocean Territory (Chagos Archipelago)\",\"fullName\":\"British Indian Ocean Territory (Chagos Archipelago)\",\"iso3\":\"IOT\",\"number\":\"086\",\"countryContinent\":\"AS\"},{\"code\":\"VG\",\"name\":\"British Virgin Islands\",\"fullName\":\"British Virgin Islands\",\"iso3\":\"VGB\",\"number\":\"092\",\"countryContinent\":\"NA\"},{\"code\":\"BN\",\"name\":\"Brunei Darussalam\",\"fullName\":\"Brunei Darussalam\",\"iso3\":\"BRN\",\"number\":\"096\",\"countryContinent\":\"AS\"},{\"code\":\"BG\",\"name\":\"Bulgaria\",\"fullName\":\"Republic of Bulgaria\",\"iso3\":\"BGR\",\"number\":\"100\",\"countryContinent\":\"EU\"},{\"code\":\"BF\",\"name\":\"Burkina Faso\",\"fullName\":\"Burkina Faso\",\"iso3\":\"BFA\",\"number\":\"854\",\"countryContinent\":\"AF\"},{\"code\":\"BI\",\"name\":\"Burundi\",\"fullName\":\"Republic of Burundi\",\"iso3\":\"BDI\",\"number\":\"108\",\"countryContinent\":\"AF\"},{\"code\":\"KH\",\"name\":\"Cambodia\",\"fullName\":\"Kingdom of Cambodia\",\"iso3\":\"KHM\",\"number\":\"116\",\"countryContinent\":\"AS\"},{\"code\":\"CM\",\"name\":\"Cameroon\",\"fullName\":\"Republic of Cameroon\",\"iso3\":\"CMR\",\"number\":\"120\",\"countryContinent\":\"AF\"},{\"code\":\"CA\",\"name\":\"Canada\",\"fullName\":\"Canada\",\"iso3\":\"CAN\",\"number\":\"124\",\"countryContinent\":\"NA\"},{\"code\":\"CV\",\"name\":\"Cape Verde\",\"fullName\":\"Republic of Cape Verde\",\"iso3\":\"CPV\",\"number\":\"132\",\"countryContinent\":\"AF\"},{\"code\":\"KY\",\"name\":\"Cayman Islands\",\"fullName\":\"Cayman Islands\",\"iso3\":\"CYM\",\"number\":\"136\",\"countryContinent\":\"NA\"},{\"code\":\"CF\",\"name\":\"Central African Republic\",\"fullName\":\"Central African Republic\",\"iso3\":\"CAF\",\"number\":\"140\",\"countryContinent\":\"AF\"},{\"code\":\"TD\",\"name\":\"Chad\",\"fullName\":\"Republic of Chad\",\"iso3\":\"TCD\",\"number\":\"148\",\"countryContinent\":\"AF\"},{\"code\":\"CL\",\"name\":\"Chile\",\"fullName\":\"Republic of Chile\",\"iso3\":\"CHL\",\"number\":\"152\",\"countryContinent\":\"SA\"},{\"code\":\"CN\",\"name\":\"China\",\"fullName\":\"People's Republic of China\",\"iso3\":\"CHN\",\"number\":\"156\",\"countryContinent\":\"AS\"},{\"code\":\"CX\",\"name\":\"Christmas Island\",\"fullName\":\"Christmas Island\",\"iso3\":\"CXR\",\"number\":\"162\",\"countryContinent\":\"AS\"},{\"code\":\"CC\",\"name\":\"Cocos (Keeling) Islands\",\"fullName\":\"Cocos (Keeling) Islands\",\"iso3\":\"CCK\",\"number\":\"166\",\"countryContinent\":\"AS\"},{\"code\":\"CO\",\"name\":\"Colombia\",\"fullName\":\"Republic of Colombia\",\"iso3\":\"COL\",\"number\":\"170\",\"countryContinent\":\"SA\"},{\"code\":\"KM\",\"name\":\"Comoros\",\"fullName\":\"Union of the Comoros\",\"iso3\":\"COM\",\"number\":\"174\",\"countryContinent\":\"AF\"},{\"code\":\"CD\",\"name\":\"Congo\",\"fullName\":\"Democratic Republic of the Congo\",\"iso3\":\"COD\",\"number\":\"180\",\"countryContinent\":\"AF\"},{\"code\":\"CG\",\"name\":\"Congo\",\"fullName\":\"Republic of the Congo\",\"iso3\":\"COG\",\"number\":\"178\",\"countryContinent\":\"AF\"},{\"code\":\"CK\",\"name\":\"Cook Islands\",\"fullName\":\"Cook Islands\",\"iso3\":\"COK\",\"number\":\"184\",\"countryContinent\":\"OC\"},{\"code\":\"CR\",\"name\":\"Costa Rica\",\"fullName\":\"Republic of Costa Rica\",\"iso3\":\"CRI\",\"number\":\"188\",\"countryContinent\":\"NA\"},{\"code\":\"CI\",\"name\":\"Cote d'Ivoire\",\"fullName\":\"Republic of Cote d'Ivoire\",\"iso3\":\"CIV\",\"number\":\"384\",\"countryContinent\":\"AF\"},{\"code\":\"HR\",\"name\":\"Croatia\",\"fullName\":\"Republic of Croatia\",\"iso3\":\"HRV\",\"number\":\"191\",\"countryContinent\":\"EU\"},{\"code\":\"CU\",\"name\":\"Cuba\",\"fullName\":\"Republic of Cuba\",\"iso3\":\"CUB\",\"number\":\"192\",\"countryContinent\":\"NA\"},{\"code\":\"CW\",\"name\":\"Curaçao\",\"fullName\":\"Curaçao\",\"iso3\":\"CUW\",\"number\":\"531\",\"countryContinent\":\"NA\"},{\"code\":\"CY\",\"name\":\"Cyprus\",\"fullName\":\"Republic of Cyprus\",\"iso3\":\"CYP\",\"number\":\"196\",\"countryContinent\":\"AS\"},{\"code\":\"CZ\",\"name\":\"Czech Republic\",\"fullName\":\"Czech Republic\",\"iso3\":\"CZE\",\"number\":\"203\",\"countryContinent\":\"EU\"},{\"code\":\"DK\",\"name\":\"Denmark\",\"fullName\":\"Kingdom of Denmark\",\"iso3\":\"DNK\",\"number\":\"208\",\"countryContinent\":\"EU\"},{\"code\":\"DJ\",\"name\":\"Djibouti\",\"fullName\":\"Republic of Djibouti\",\"iso3\":\"DJI\",\"number\":\"262\",\"countryContinent\":\"AF\"},{\"code\":\"DM\",\"name\":\"Dominica\",\"fullName\":\"Commonwealth of Dominica\",\"iso3\":\"DMA\",\"number\":\"212\",\"countryContinent\":\"NA\"},{\"code\":\"DO\",\"name\":\"Dominican Republic\",\"fullName\":\"Dominican Republic\",\"iso3\":\"DOM\",\"number\":\"214\",\"countryContinent\":\"NA\"},{\"code\":\"EC\",\"name\":\"Ecuador\",\"fullName\":\"Republic of Ecuador\",\"iso3\":\"ECU\",\"number\":\"218\",\"countryContinent\":\"SA\"},{\"code\":\"EG\",\"name\":\"Egypt\",\"fullName\":\"Arab Republic of Egypt\",\"iso3\":\"EGY\",\"number\":\"818\",\"countryContinent\":\"AF\"},{\"code\":\"SV\",\"name\":\"El Salvador\",\"fullName\":\"Republic of El Salvador\",\"iso3\":\"SLV\",\"number\":\"222\",\"countryContinent\":\"NA\"},{\"code\":\"GQ\",\"name\":\"Equatorial Guinea\",\"fullName\":\"Republic of Equatorial Guinea\",\"iso3\":\"GNQ\",\"number\":\"226\",\"countryContinent\":\"AF\"},{\"code\":\"ER\",\"name\":\"Eritrea\",\"fullName\":\"State of Eritrea\",\"iso3\":\"ERI\",\"number\":\"232\",\"countryContinent\":\"AF\"},{\"code\":\"EE\",\"name\":\"Estonia\",\"fullName\":\"Republic of Estonia\",\"iso3\":\"EST\",\"number\":\"233\",\"countryContinent\":\"EU\"},{\"code\":\"ET\",\"name\":\"Ethiopia\",\"fullName\":\"Federal Democratic Republic of Ethiopia\",\"iso3\":\"ETH\",\"number\":\"231\",\"countryContinent\":\"AF\"},{\"code\":\"FO\",\"name\":\"Faroe Islands\",\"fullName\":\"Faroe Islands\",\"iso3\":\"FRO\",\"number\":\"234\",\"countryContinent\":\"EU\"},{\"code\":\"FK\",\"name\":\"Falkland Islands (Malvinas)\",\"fullName\":\"Falkland Islands (Malvinas)\",\"iso3\":\"FLK\",\"number\":\"238\",\"countryContinent\":\"SA\"},{\"code\":\"FJ\",\"name\":\"Fiji\",\"fullName\":\"Republic of Fiji\",\"iso3\":\"FJI\",\"number\":\"242\",\"countryContinent\":\"OC\"},{\"code\":\"FI\",\"name\":\"Finland\",\"fullName\":\"Republic of Finland\",\"iso3\":\"FIN\",\"number\":\"246\",\"countryContinent\":\"EU\"},{\"code\":\"FR\",\"name\":\"France\",\"fullName\":\"French Republic\",\"iso3\":\"FRA\",\"number\":\"250\",\"countryContinent\":\"EU\"},{\"code\":\"GF\",\"name\":\"French Guiana\",\"fullName\":\"French Guiana\",\"iso3\":\"GUF\",\"number\":\"254\",\"countryContinent\":\"SA\"},{\"code\":\"PF\",\"name\":\"French Polynesia\",\"fullName\":\"French Polynesia\",\"iso3\":\"PYF\",\"number\":\"258\",\"countryContinent\":\"OC\"},{\"code\":\"TF\",\"name\":\"French Southern Territories\",\"fullName\":\"French Southern Territories\",\"iso3\":\"ATF\",\"number\":\"260\",\"countryContinent\":\"AN\"},{\"code\":\"GA\",\"name\":\"Gabon\",\"fullName\":\"Gabonese Republic\",\"iso3\":\"GAB\",\"number\":\"266\",\"countryContinent\":\"AF\"},{\"code\":\"GM\",\"name\":\"Gambia\",\"fullName\":\"Republic of the Gambia\",\"iso3\":\"GMB\",\"number\":\"270\",\"countryContinent\":\"AF\"},{\"code\":\"GE\",\"name\":\"Georgia\",\"fullName\":\"Georgia\",\"iso3\":\"GEO\",\"number\":\"268\",\"countryContinent\":\"AS\"},{\"code\":\"DE\",\"name\":\"Germany\",\"fullName\":\"Federal Republic of Germany\",\"iso3\":\"DEU\",\"number\":\"276\",\"countryContinent\":\"EU\"},{\"code\":\"GH\",\"name\":\"Ghana\",\"fullName\":\"Republic of Ghana\",\"iso3\":\"GHA\",\"number\":\"288\",\"countryContinent\":\"AF\"},{\"code\":\"GI\",\"name\":\"Gibraltar\",\"fullName\":\"Gibraltar\",\"iso3\":\"GIB\",\"number\":\"292\",\"countryContinent\":\"EU\"},{\"code\":\"GR\",\"name\":\"Greece\",\"fullName\":\"Hellenic Republic Greece\",\"iso3\":\"GRC\",\"number\":\"300\",\"countryContinent\":\"EU\"},{\"code\":\"GL\",\"name\":\"Greenland\",\"fullName\":\"Greenland\",\"iso3\":\"GRL\",\"number\":\"304\",\"countryContinent\":\"NA\"},{\"code\":\"GD\",\"name\":\"Grenada\",\"fullName\":\"Grenada\",\"iso3\":\"GRD\",\"number\":\"308\",\"countryContinent\":\"NA\"},{\"code\":\"GP\",\"name\":\"Guadeloupe\",\"fullName\":\"Guadeloupe\",\"iso3\":\"GLP\",\"number\":\"312\",\"countryContinent\":\"NA\"},{\"code\":\"GU\",\"name\":\"Guam\",\"fullName\":\"Guam\",\"iso3\":\"GUM\",\"number\":\"316\",\"countryContinent\":\"OC\"},{\"code\":\"GT\",\"name\":\"Guatemala\",\"fullName\":\"Republic of Guatemala\",\"iso3\":\"GTM\",\"number\":\"320\",\"countryContinent\":\"NA\"},{\"code\":\"GG\",\"name\":\"Guernsey\",\"fullName\":\"Bailiwick of Guernsey\",\"iso3\":\"GGY\",\"number\":\"831\",\"countryContinent\":\"EU\"},{\"code\":\"GN\",\"name\":\"Guinea\",\"fullName\":\"Republic of Guinea\",\"iso3\":\"GIN\",\"number\":\"324\",\"countryContinent\":\"AF\"},{\"code\":\"GW\",\"name\":\"Guinea-Bissau\",\"fullName\":\"Republic of Guinea-Bissau\",\"iso3\":\"GNB\",\"number\":\"624\",\"countryContinent\":\"AF\"},{\"code\":\"GY\",\"name\":\"Guyana\",\"fullName\":\"Co-operative Republic of Guyana\",\"iso3\":\"GUY\",\"number\":\"328\",\"countryContinent\":\"SA\"},{\"code\":\"HT\",\"name\":\"Haiti\",\"fullName\":\"Republic of Haiti\",\"iso3\":\"HTI\",\"number\":\"332\",\"countryContinent\":\"NA\"},{\"code\":\"HM\",\"name\":\"Heard Island and McDonald Islands\",\"fullName\":\"Heard Island and McDonald Islands\",\"iso3\":\"HMD\",\"number\":\"334\",\"countryContinent\":\"AN\"},{\"code\":\"VA\",\"name\":\"Holy See (Vatican City State)\",\"fullName\":\"Holy See (Vatican City State)\",\"iso3\":\"VAT\",\"number\":\"336\",\"countryContinent\":\"EU\"},{\"code\":\"HN\",\"name\":\"Honduras\",\"fullName\":\"Republic of Honduras\",\"iso3\":\"HND\",\"number\":\"340\",\"countryContinent\":\"NA\"},{\"code\":\"HK\",\"name\":\"Hong Kong\",\"fullName\":\"Hong Kong Special Administrative Region of China\",\"iso3\":\"HKG\",\"number\":\"344\",\"countryContinent\":\"AS\"},{\"code\":\"HU\",\"name\":\"Hungary\",\"fullName\":\"Hungary\",\"iso3\":\"HUN\",\"number\":\"348\",\"countryContinent\":\"EU\"},{\"code\":\"IS\",\"name\":\"Iceland\",\"fullName\":\"Republic of Iceland\",\"iso3\":\"ISL\",\"number\":\"352\",\"countryContinent\":\"EU\"},{\"code\":\"IN\",\"name\":\"India\",\"fullName\":\"Republic of India\",\"iso3\":\"IND\",\"number\":\"356\",\"countryContinent\":\"AS\"},{\"code\":\"ID\",\"name\":\"Indonesia\",\"fullName\":\"Republic of Indonesia\",\"iso3\":\"IDN\",\"number\":\"360\",\"countryContinent\":\"AS\"},{\"code\":\"IR\",\"name\":\"Iran\",\"fullName\":\"Islamic Republic of Iran\",\"iso3\":\"IRN\",\"number\":\"364\",\"countryContinent\":\"AS\"},{\"code\":\"IQ\",\"name\":\"Iraq\",\"fullName\":\"Republic of Iraq\",\"iso3\":\"IRQ\",\"number\":\"368\",\"countryContinent\":\"AS\"},{\"code\":\"IE\",\"name\":\"Ireland\",\"fullName\":\"Ireland\",\"iso3\":\"IRL\",\"number\":\"372\",\"countryContinent\":\"EU\"},{\"code\":\"IM\",\"name\":\"Isle of Man\",\"fullName\":\"Isle of Man\",\"iso3\":\"IMN\",\"number\":\"833\",\"countryContinent\":\"EU\"},{\"code\":\"IL\",\"name\":\"Israel\",\"fullName\":\"State of Israel\",\"iso3\":\"ISR\",\"number\":\"376\",\"countryContinent\":\"AS\"},{\"code\":\"IT\",\"name\":\"Italy\",\"fullName\":\"Italian Republic\",\"iso3\":\"ITA\",\"number\":\"380\",\"countryContinent\":\"EU\"},{\"code\":\"JM\",\"name\":\"Jamaica\",\"fullName\":\"Jamaica\",\"iso3\":\"JAM\",\"number\":\"388\",\"countryContinent\":\"NA\"},{\"code\":\"JP\",\"name\":\"Japan\",\"fullName\":\"Japan\",\"iso3\":\"JPN\",\"number\":\"392\",\"countryContinent\":\"AS\"},{\"code\":\"JE\",\"name\":\"Jersey\",\"fullName\":\"Bailiwick of Jersey\",\"iso3\":\"JEY\",\"number\":\"832\",\"countryContinent\":\"EU\"},{\"code\":\"JO\",\"name\":\"Jordan\",\"fullName\":\"Hashemite Kingdom of Jordan\",\"iso3\":\"JOR\",\"number\":\"400\",\"countryContinent\":\"AS\"},{\"code\":\"KZ\",\"name\":\"Kazakhstan\",\"fullName\":\"Republic of Kazakhstan\",\"iso3\":\"KAZ\",\"number\":\"398\",\"countryContinent\":\"AS\"},{\"code\":\"KE\",\"name\":\"Kenya\",\"fullName\":\"Republic of Kenya\",\"iso3\":\"KEN\",\"number\":\"404\",\"countryContinent\":\"AF\"},{\"code\":\"KI\",\"name\":\"Kiribati\",\"fullName\":\"Republic of Kiribati\",\"iso3\":\"KIR\",\"number\":\"296\",\"countryContinent\":\"OC\"},{\"code\":\"KP\",\"name\":\"Korea\",\"fullName\":\"Democratic People's Republic of Korea\",\"iso3\":\"PRK\",\"number\":\"408\",\"countryContinent\":\"AS\"},{\"code\":\"KR\",\"name\":\"Korea\",\"fullName\":\"Republic of Korea\",\"iso3\":\"KOR\",\"number\":\"410\",\"countryContinent\":\"AS\"},{\"code\":\"KW\",\"name\":\"Kuwait\",\"fullName\":\"State of Kuwait\",\"iso3\":\"KWT\",\"number\":\"414\",\"countryContinent\":\"AS\"},{\"code\":\"KG\",\"name\":\"Kyrgyz Republic\",\"fullName\":\"Kyrgyz Republic\",\"iso3\":\"KGZ\",\"number\":\"417\",\"countryContinent\":\"AS\"},{\"code\":\"LA\",\"name\":\"Lao People's Democratic Republic\",\"fullName\":\"Lao People's Democratic Republic\",\"iso3\":\"LAO\",\"number\":\"418\",\"countryContinent\":\"AS\"},{\"code\":\"LV\",\"name\":\"Latvia\",\"fullName\":\"Republic of Latvia\",\"iso3\":\"LVA\",\"number\":\"428\",\"countryContinent\":\"EU\"},{\"code\":\"LB\",\"name\":\"Lebanon\",\"fullName\":\"Lebanese Republic\",\"iso3\":\"LBN\",\"number\":\"422\",\"countryContinent\":\"AS\"},{\"code\":\"LS\",\"name\":\"Lesotho\",\"fullName\":\"Kingdom of Lesotho\",\"iso3\":\"LSO\",\"number\":\"426\",\"countryContinent\":\"AF\"},{\"code\":\"LR\",\"name\":\"Liberia\",\"fullName\":\"Republic of Liberia\",\"iso3\":\"LBR\",\"number\":\"430\",\"countryContinent\":\"AF\"},{\"code\":\"LY\",\"name\":\"Libya\",\"fullName\":\"Libya\",\"iso3\":\"LBY\",\"number\":\"434\",\"countryContinent\":\"AF\"},{\"code\":\"LI\",\"name\":\"Liechtenstein\",\"fullName\":\"Principality of Liechtenstein\",\"iso3\":\"LIE\",\"number\":\"438\",\"countryContinent\":\"EU\"},{\"code\":\"LT\",\"name\":\"Lithuania\",\"fullName\":\"Republic of Lithuania\",\"iso3\":\"LTU\",\"number\":\"440\",\"countryContinent\":\"EU\"},{\"code\":\"LU\",\"name\":\"Luxembourg\",\"fullName\":\"Grand Duchy of Luxembourg\",\"iso3\":\"LUX\",\"number\":\"442\",\"countryContinent\":\"EU\"},{\"code\":\"MO\",\"name\":\"Macao\",\"fullName\":\"Macao Special Administrative Region of China\",\"iso3\":\"MAC\",\"number\":\"446\",\"countryContinent\":\"AS\"},{\"code\":\"MK\",\"name\":\"Macedonia\",\"fullName\":\"Republic of Macedonia\",\"iso3\":\"MKD\",\"number\":\"807\",\"countryContinent\":\"EU\"},{\"code\":\"MG\",\"name\":\"Madagascar\",\"fullName\":\"Republic of Madagascar\",\"iso3\":\"MDG\",\"number\":\"450\",\"countryContinent\":\"AF\"},{\"code\":\"MW\",\"name\":\"Malawi\",\"fullName\":\"Republic of Malawi\",\"iso3\":\"MWI\",\"number\":\"454\",\"countryContinent\":\"AF\"},{\"code\":\"MY\",\"name\":\"Malaysia\",\"fullName\":\"Malaysia\",\"iso3\":\"MYS\",\"number\":\"458\",\"countryContinent\":\"AS\"},{\"code\":\"MV\",\"name\":\"Maldives\",\"fullName\":\"Republic of Maldives\",\"iso3\":\"MDV\",\"number\":\"462\",\"countryContinent\":\"AS\"},{\"code\":\"ML\",\"name\":\"Mali\",\"fullName\":\"Republic of Mali\",\"iso3\":\"MLI\",\"number\":\"466\",\"countryContinent\":\"AF\"},{\"code\":\"MT\",\"name\":\"Malta\",\"fullName\":\"Republic of Malta\",\"iso3\":\"MLT\",\"number\":\"470\",\"countryContinent\":\"EU\"},{\"code\":\"MH\",\"name\":\"Marshall Islands\",\"fullName\":\"Republic of the Marshall Islands\",\"iso3\":\"MHL\",\"number\":\"584\",\"countryContinent\":\"OC\"},{\"code\":\"MQ\",\"name\":\"Martinique\",\"fullName\":\"Martinique\",\"iso3\":\"MTQ\",\"number\":\"474\",\"countryContinent\":\"NA\"},{\"code\":\"MR\",\"name\":\"Mauritania\",\"fullName\":\"Islamic Republic of Mauritania\",\"iso3\":\"MRT\",\"number\":\"478\",\"countryContinent\":\"AF\"},{\"code\":\"MU\",\"name\":\"Mauritius\",\"fullName\":\"Republic of Mauritius\",\"iso3\":\"MUS\",\"number\":\"480\",\"countryContinent\":\"AF\"},{\"code\":\"YT\",\"name\":\"Mayotte\",\"fullName\":\"Mayotte\",\"iso3\":\"MYT\",\"number\":\"175\",\"countryContinent\":\"AF\"},{\"code\":\"MX\",\"name\":\"Mexico\",\"fullName\":\"United Mexican States\",\"iso3\":\"MEX\",\"number\":\"484\",\"countryContinent\":\"NA\"},{\"code\":\"FM\",\"name\":\"Micronesia\",\"fullName\":\"Federated States of Micronesia\",\"iso3\":\"FSM\",\"number\":\"583\",\"countryContinent\":\"OC\"},{\"code\":\"MD\",\"name\":\"Moldova\",\"fullName\":\"Republic of Moldova\",\"iso3\":\"MDA\",\"number\":\"498\",\"countryContinent\":\"EU\"},{\"code\":\"MC\",\"name\":\"Monaco\",\"fullName\":\"Principality of Monaco\",\"iso3\":\"MCO\",\"number\":\"492\",\"countryContinent\":\"EU\"},{\"code\":\"MN\",\"name\":\"Mongolia\",\"fullName\":\"Mongolia\",\"iso3\":\"MNG\",\"number\":\"496\",\"countryContinent\":\"AS\"},{\"code\":\"ME\",\"name\":\"Montenegro\",\"fullName\":\"Montenegro\",\"iso3\":\"MNE\",\"number\":\"499\",\"countryContinent\":\"EU\"},{\"code\":\"MS\",\"name\":\"Montserrat\",\"fullName\":\"Montserrat\",\"iso3\":\"MSR\",\"number\":\"500\",\"countryContinent\":\"NA\"},{\"code\":\"MA\",\"name\":\"Morocco\",\"fullName\":\"Kingdom of Morocco\",\"iso3\":\"MAR\",\"number\":\"504\",\"countryContinent\":\"AF\"},{\"code\":\"MZ\",\"name\":\"Mozambique\",\"fullName\":\"Republic of Mozambique\",\"iso3\":\"MOZ\",\"number\":\"508\",\"countryContinent\":\"AF\"},{\"code\":\"MM\",\"name\":\"Myanmar\",\"fullName\":\"Republic of the Union of Myanmar\",\"iso3\":\"MMR\",\"number\":\"104\",\"countryContinent\":\"AS\"},{\"code\":\"NA\",\"name\":\"Namibia\",\"fullName\":\"Republic of Namibia\",\"iso3\":\"NAM\",\"number\":\"516\",\"countryContinent\":\"AF\"},{\"code\":\"NR\",\"name\":\"Nauru\",\"fullName\":\"Republic of Nauru\",\"iso3\":\"NRU\",\"number\":\"520\",\"countryContinent\":\"OC\"},{\"code\":\"NP\",\"name\":\"Nepal\",\"fullName\":\"Federal Democratic Republic of Nepal\",\"iso3\":\"NPL\",\"number\":\"524\",\"countryContinent\":\"AS\"},{\"code\":\"NL\",\"name\":\"Netherlands\",\"fullName\":\"Kingdom of the Netherlands\",\"iso3\":\"NLD\",\"number\":\"528\",\"countryContinent\":\"EU\"},{\"code\":\"NC\",\"name\":\"New Caledonia\",\"fullName\":\"New Caledonia\",\"iso3\":\"NCL\",\"number\":\"540\",\"countryContinent\":\"OC\"},{\"code\":\"NZ\",\"name\":\"New Zealand\",\"fullName\":\"New Zealand\",\"iso3\":\"NZL\",\"number\":\"554\",\"countryContinent\":\"OC\"},{\"code\":\"NI\",\"name\":\"Nicaragua\",\"fullName\":\"Republic of Nicaragua\",\"iso3\":\"NIC\",\"number\":\"558\",\"countryContinent\":\"NA\"},{\"code\":\"NE\",\"name\":\"Niger\",\"fullName\":\"Republic of Niger\",\"iso3\":\"NER\",\"number\":\"562\",\"countryContinent\":\"AF\"},{\"code\":\"NG\",\"name\":\"Nigeria\",\"fullName\":\"Federal Republic of Nigeria\",\"iso3\":\"NGA\",\"number\":\"566\",\"countryContinent\":\"AF\"},{\"code\":\"NU\",\"name\":\"Niue\",\"fullName\":\"Niue\",\"iso3\":\"NIU\",\"number\":\"570\",\"countryContinent\":\"OC\"},{\"code\":\"NF\",\"name\":\"Norfolk Island\",\"fullName\":\"Norfolk Island\",\"iso3\":\"NFK\",\"number\":\"574\",\"countryContinent\":\"OC\"},{\"code\":\"MP\",\"name\":\"Northern Mariana Islands\",\"fullName\":\"Commonwealth of the Northern Mariana Islands\",\"iso3\":\"MNP\",\"number\":\"580\",\"countryContinent\":\"OC\"},{\"code\":\"NO\",\"name\":\"Norway\",\"fullName\":\"Kingdom of Norway\",\"iso3\":\"NOR\",\"number\":\"578\",\"countryContinent\":\"EU\"},{\"code\":\"OM\",\"name\":\"Oman\",\"fullName\":\"Sultanate of Oman\",\"iso3\":\"OMN\",\"number\":\"512\",\"countryContinent\":\"AS\"},{\"code\":\"PK\",\"name\":\"Pakistan\",\"fullName\":\"Islamic Republic of Pakistan\",\"iso3\":\"PAK\",\"number\":\"586\",\"countryContinent\":\"AS\"},{\"code\":\"PW\",\"name\":\"Palau\",\"fullName\":\"Republic of Palau\",\"iso3\":\"PLW\",\"number\":\"585\",\"countryContinent\":\"OC\"},{\"code\":\"PS\",\"name\":\"Palestinian Territory\",\"fullName\":\"Occupied Palestinian Territory\",\"iso3\":\"PSE\",\"number\":\"275\",\"countryContinent\":\"AS\"},{\"code\":\"PA\",\"name\":\"Panama\",\"fullName\":\"Republic of Panama\",\"iso3\":\"PAN\",\"number\":\"591\",\"countryContinent\":\"NA\"},{\"code\":\"PG\",\"name\":\"Papua New Guinea\",\"fullName\":\"Independent State of Papua New Guinea\",\"iso3\":\"PNG\",\"number\":\"598\",\"countryContinent\":\"OC\"},{\"code\":\"PY\",\"name\":\"Paraguay\",\"fullName\":\"Republic of Paraguay\",\"iso3\":\"PRY\",\"number\":\"600\",\"countryContinent\":\"SA\"},{\"code\":\"PE\",\"name\":\"Peru\",\"fullName\":\"Republic of Peru\",\"iso3\":\"PER\",\"number\":\"604\",\"countryContinent\":\"SA\"},{\"code\":\"PH\",\"name\":\"Philippines\",\"fullName\":\"Republic of the Philippines\",\"iso3\":\"PHL\",\"number\":\"608\",\"countryContinent\":\"AS\"},{\"code\":\"PN\",\"name\":\"Pitcairn Islands\",\"fullName\":\"Pitcairn Islands\",\"iso3\":\"PCN\",\"number\":\"612\",\"countryContinent\":\"OC\"},{\"code\":\"PL\",\"name\":\"Poland\",\"fullName\":\"Republic of Poland\",\"iso3\":\"POL\",\"number\":\"616\",\"countryContinent\":\"EU\"},{\"code\":\"PT\",\"name\":\"Portugal\",\"fullName\":\"Portuguese Republic\",\"iso3\":\"PRT\",\"number\":\"620\",\"countryContinent\":\"EU\"},{\"code\":\"PR\",\"name\":\"Puerto Rico\",\"fullName\":\"Commonwealth of Puerto Rico\",\"iso3\":\"PRI\",\"number\":\"630\",\"countryContinent\":\"NA\"},{\"code\":\"QA\",\"name\":\"Qatar\",\"fullName\":\"State of Qatar\",\"iso3\":\"QAT\",\"number\":\"634\",\"countryContinent\":\"AS\"},{\"code\":\"RE\",\"name\":\"Réunion\",\"fullName\":\"Réunion\",\"iso3\":\"REU\",\"number\":\"638\",\"countryContinent\":\"AF\"},{\"code\":\"RW\",\"name\":\"Rwanda\",\"fullName\":\"Republic of Rwanda\",\"iso3\":\"RWA\",\"number\":\"646\",\"countryContinent\":\"AF\"},{\"code\":\"BL\",\"name\":\"Saint Barthélemy\",\"fullName\":\"Saint Barthélemy\",\"iso3\":\"BLM\",\"number\":\"652\",\"countryContinent\":\"NA\"},{\"code\":\"SH\",\"name\":\"Saint Helena, Ascension and Tristan da Cunha\",\"fullName\":\"Saint Helena, Ascension and Tristan da Cunha\",\"iso3\":\"SHN\",\"number\":\"654\",\"countryContinent\":\"AF\"},{\"code\":\"KN\",\"name\":\"Saint Kitts and Nevis\",\"fullName\":\"Federation of Saint Kitts and Nevis\",\"iso3\":\"KNA\",\"number\":\"659\",\"countryContinent\":\"NA\"},{\"code\":\"LC\",\"name\":\"Saint Lucia\",\"fullName\":\"Saint Lucia\",\"iso3\":\"LCA\",\"number\":\"662\",\"countryContinent\":\"NA\"},{\"code\":\"MF\",\"name\":\"Saint Martin\",\"fullName\":\"Saint Martin (French part)\",\"iso3\":\"MAF\",\"number\":\"663\",\"countryContinent\":\"NA\"},{\"code\":\"PM\",\"name\":\"Saint Pierre and Miquelon\",\"fullName\":\"Saint Pierre and Miquelon\",\"iso3\":\"SPM\",\"number\":\"666\",\"countryContinent\":\"NA\"},{\"code\":\"VC\",\"name\":\"Saint Vincent and the Grenadines\",\"fullName\":\"Saint Vincent and the Grenadines\",\"iso3\":\"VCT\",\"number\":\"670\",\"countryContinent\":\"NA\"},{\"code\":\"WS\",\"name\":\"Samoa\",\"fullName\":\"Independent State of Samoa\",\"iso3\":\"WSM\",\"number\":\"882\",\"countryContinent\":\"OC\"},{\"code\":\"SM\",\"name\":\"San Marino\",\"fullName\":\"Republic of San Marino\",\"iso3\":\"SMR\",\"number\":\"674\",\"countryContinent\":\"EU\"},{\"code\":\"ST\",\"name\":\"Sao Tome and Principe\",\"fullName\":\"Democratic Republic of Sao Tome and Principe\",\"iso3\":\"STP\",\"number\":\"678\",\"countryContinent\":\"AF\"},{\"code\":\"SA\",\"name\":\"Saudi Arabia\",\"fullName\":\"Kingdom of Saudi Arabia\",\"iso3\":\"SAU\",\"number\":\"682\",\"countryContinent\":\"AS\"},{\"code\":\"SN\",\"name\":\"Senegal\",\"fullName\":\"Republic of Senegal\",\"iso3\":\"SEN\",\"number\":\"686\",\"countryContinent\":\"AF\"},{\"code\":\"RS\",\"name\":\"Serbia\",\"fullName\":\"Republic of Serbia\",\"iso3\":\"SRB\",\"number\":\"688\",\"countryContinent\":\"EU\"},{\"code\":\"SC\",\"name\":\"Seychelles\",\"fullName\":\"Republic of Seychelles\",\"iso3\":\"SYC\",\"number\":\"690\",\"countryContinent\":\"AF\"},{\"code\":\"SL\",\"name\":\"Sierra Leone\",\"fullName\":\"Republic of Sierra Leone\",\"iso3\":\"SLE\",\"number\":\"694\",\"countryContinent\":\"AF\"},{\"code\":\"SG\",\"name\":\"Singapore\",\"fullName\":\"Republic of Singapore\",\"iso3\":\"SGP\",\"number\":\"702\",\"countryContinent\":\"AS\"},{\"code\":\"SX\",\"name\":\"Sint Maarten (Dutch part)\",\"fullName\":\"Sint Maarten (Dutch part)\",\"iso3\":\"SXM\",\"number\":\"534\",\"countryContinent\":\"NA\"},{\"code\":\"SK\",\"name\":\"Slovakia (Slovak Republic)\",\"fullName\":\"Slovakia (Slovak Republic)\",\"iso3\":\"SVK\",\"number\":\"703\",\"countryContinent\":\"EU\"},{\"code\":\"SI\",\"name\":\"Slovenia\",\"fullName\":\"Republic of Slovenia\",\"iso3\":\"SVN\",\"number\":\"705\",\"countryContinent\":\"EU\"},{\"code\":\"SB\",\"name\":\"Solomon Islands\",\"fullName\":\"Solomon Islands\",\"iso3\":\"SLB\",\"number\":\"090\",\"countryContinent\":\"OC\"},{\"code\":\"SO\",\"name\":\"Somalia\",\"fullName\":\"Somali Republic\",\"iso3\":\"SOM\",\"number\":\"706\",\"countryContinent\":\"AF\"},{\"code\":\"ZA\",\"name\":\"South Africa\",\"fullName\":\"Republic of South Africa\",\"iso3\":\"ZAF\",\"number\":\"710\",\"countryContinent\":\"AF\"},{\"code\":\"GS\",\"name\":\"South Georgia and the South Sandwich Islands\",\"fullName\":\"South Georgia and the South Sandwich Islands\",\"iso3\":\"SGS\",\"number\":\"239\",\"countryContinent\":\"AN\"},{\"code\":\"SS\",\"name\":\"South Sudan\",\"fullName\":\"Republic of South Sudan\",\"iso3\":\"SSD\",\"number\":\"728\",\"countryContinent\":\"AF\"},{\"code\":\"ES\",\"name\":\"Spain\",\"fullName\":\"Kingdom of Spain\",\"iso3\":\"ESP\",\"number\":\"724\",\"countryContinent\":\"EU\"},{\"code\":\"LK\",\"name\":\"Sri Lanka\",\"fullName\":\"Democratic Socialist Republic of Sri Lanka\",\"iso3\":\"LKA\",\"number\":\"144\",\"countryContinent\":\"AS\"},{\"code\":\"SD\",\"name\":\"Sudan\",\"fullName\":\"Republic of Sudan\",\"iso3\":\"SDN\",\"number\":\"729\",\"countryContinent\":\"AF\"},{\"code\":\"SR\",\"name\":\"Suriname\",\"fullName\":\"Republic of Suriname\",\"iso3\":\"SUR\",\"number\":\"740\",\"countryContinent\":\"SA\"},{\"code\":\"SJ\",\"name\":\"Svalbard & Jan Mayen Islands\",\"fullName\":\"Svalbard & Jan Mayen Islands\",\"iso3\":\"SJM\",\"number\":\"744\",\"countryContinent\":\"EU\"},{\"code\":\"SZ\",\"name\":\"Swaziland\",\"fullName\":\"Kingdom of Swaziland\",\"iso3\":\"SWZ\",\"number\":\"748\",\"countryContinent\":\"AF\"},{\"code\":\"SE\",\"name\":\"Sweden\",\"fullName\":\"Kingdom of Sweden\",\"iso3\":\"SWE\",\"number\":\"752\",\"countryContinent\":\"EU\"},{\"code\":\"CH\",\"name\":\"Switzerland\",\"fullName\":\"Swiss Confederation\",\"iso3\":\"CHE\",\"number\":\"756\",\"countryContinent\":\"EU\"},{\"code\":\"SY\",\"name\":\"Syrian Arab Republic\",\"fullName\":\"Syrian Arab Republic\",\"iso3\":\"SYR\",\"number\":\"760\",\"countryContinent\":\"AS\"},{\"code\":\"TW\",\"name\":\"Taiwan\",\"fullName\":\"Taiwan, Province of China\",\"iso3\":\"TWN\",\"number\":\"158\",\"countryContinent\":\"AS\"},{\"code\":\"TJ\",\"name\":\"Tajikistan\",\"fullName\":\"Republic of Tajikistan\",\"iso3\":\"TJK\",\"number\":\"762\",\"countryContinent\":\"AS\"},{\"code\":\"TZ\",\"name\":\"Tanzania\",\"fullName\":\"United Republic of Tanzania\",\"iso3\":\"TZA\",\"number\":\"834\",\"countryContinent\":\"AF\"},{\"code\":\"TH\",\"name\":\"Thailand\",\"fullName\":\"Kingdom of Thailand\",\"iso3\":\"THA\",\"number\":\"764\",\"countryContinent\":\"AS\"},{\"code\":\"TL\",\"name\":\"Timor-Leste\",\"fullName\":\"Democratic Republic of Timor-Leste\",\"iso3\":\"TLS\",\"number\":\"626\",\"countryContinent\":\"AS\"},{\"code\":\"TG\",\"name\":\"Togo\",\"fullName\":\"Togolese Republic\",\"iso3\":\"TGO\",\"number\":\"768\",\"countryContinent\":\"AF\"},{\"code\":\"TK\",\"name\":\"Tokelau\",\"fullName\":\"Tokelau\",\"iso3\":\"TKL\",\"number\":\"772\",\"countryContinent\":\"OC\"},{\"code\":\"TO\",\"name\":\"Tonga\",\"fullName\":\"Kingdom of Tonga\",\"iso3\":\"TON\",\"number\":\"776\",\"countryContinent\":\"OC\"},{\"code\":\"TT\",\"name\":\"Trinidad and Tobago\",\"fullName\":\"Republic of Trinidad and Tobago\",\"iso3\":\"TTO\",\"number\":\"780\",\"countryContinent\":\"NA\"},{\"code\":\"TN\",\"name\":\"Tunisia\",\"fullName\":\"Tunisian Republic\",\"iso3\":\"TUN\",\"number\":\"788\",\"countryContinent\":\"AF\"},{\"code\":\"TR\",\"name\":\"Turkey\",\"fullName\":\"Republic of Turkey\",\"iso3\":\"TUR\",\"number\":\"792\",\"countryContinent\":\"AS\"},{\"code\":\"TM\",\"name\":\"Turkmenistan\",\"fullName\":\"Turkmenistan\",\"iso3\":\"TKM\",\"number\":\"795\",\"countryContinent\":\"AS\"},{\"code\":\"TC\",\"name\":\"Turks and Caicos Islands\",\"fullName\":\"Turks and Caicos Islands\",\"iso3\":\"TCA\",\"number\":\"796\",\"countryContinent\":\"NA\"},{\"code\":\"TV\",\"name\":\"Tuvalu\",\"fullName\":\"Tuvalu\",\"iso3\":\"TUV\",\"number\":\"798\",\"countryContinent\":\"OC\"},{\"code\":\"UG\",\"name\":\"Uganda\",\"fullName\":\"Republic of Uganda\",\"iso3\":\"UGA\",\"number\":\"800\",\"countryContinent\":\"AF\"},{\"code\":\"UA\",\"name\":\"Ukraine\",\"fullName\":\"Ukraine\",\"iso3\":\"UKR\",\"number\":\"804\",\"countryContinent\":\"EU\"},{\"code\":\"AE\",\"name\":\"United Arab Emirates\",\"fullName\":\"United Arab Emirates\",\"iso3\":\"ARE\",\"number\":\"784\",\"countryContinent\":\"AS\"},{\"code\":\"GB\",\"name\":\"United Kingdom of Great Britain & Northern Ireland\",\"fullName\":\"United Kingdom of Great Britain & Northern Ireland\",\"iso3\":\"GBR\",\"number\":\"826\",\"countryContinent\":\"EU\"},{\"code\":\"US\",\"name\":\"United States of America\",\"fullName\":\"United States of America\",\"iso3\":\"USA\",\"number\":\"840\",\"countryContinent\":\"NA\"},{\"code\":\"UM\",\"name\":\"United States Minor Outlying Islands\",\"fullName\":\"United States Minor Outlying Islands\",\"iso3\":\"UMI\",\"number\":\"581\",\"countryContinent\":\"OC\"},{\"code\":\"VI\",\"name\":\"United States Virgin Islands\",\"fullName\":\"United States Virgin Islands\",\"iso3\":\"VIR\",\"number\":\"850\",\"countryContinent\":\"NA\"},{\"code\":\"UY\",\"name\":\"Uruguay\",\"fullName\":\"Eastern Republic of Uruguay\",\"iso3\":\"URY\",\"number\":\"858\",\"countryContinent\":\"SA\"},{\"code\":\"UZ\",\"name\":\"Uzbekistan\",\"fullName\":\"Republic of Uzbekistan\",\"iso3\":\"UZB\",\"number\":\"860\",\"countryContinent\":\"AS\"},{\"code\":\"VU\",\"name\":\"Vanuatu\",\"fullName\":\"Republic of Vanuatu\",\"iso3\":\"VUT\",\"number\":\"548\",\"countryContinent\":\"OC\"},{\"code\":\"VE\",\"name\":\"Venezuela\",\"fullName\":\"Bolivarian Republic of Venezuela\",\"iso3\":\"VEN\",\"number\":\"862\",\"countryContinent\":\"SA\"},{\"code\":\"VN\",\"name\":\"Vietnam\",\"fullName\":\"Socialist Republic of Vietnam\",\"iso3\":\"VNM\",\"number\":\"704\",\"countryContinent\":\"AS\"},{\"code\":\"WF\",\"name\":\"Wallis and Futuna\",\"fullName\":\"Wallis and Futuna\",\"iso3\":\"WLF\",\"number\":\"876\",\"countryContinent\":\"OC\"},{\"code\":\"EH\",\"name\":\"Western Sahara\",\"fullName\":\"Western Sahara\",\"iso3\":\"ESH\",\"number\":\"732\",\"countryContinent\":\"AF\"},{\"code\":\"YE\",\"name\":\"Yemen\",\"fullName\":\"Yemen\",\"iso3\":\"YEM\",\"number\":\"887\",\"countryContinent\":\"AS\"},{\"code\":\"ZM\",\"name\":\"Zambia\",\"fullName\":\"Republic of Zambia\",\"iso3\":\"ZMB\",\"number\":\"894\",\"countryContinent\":\"AF\"},{\"code\":\"ZW\",\"name\":\"Zimbabwe\",\"fullName\":\"Republic of Zimbabwe\",\"iso3\":\"ZWE\",\"number\":\"716\",\"countryContinent\":\"AF\"}]";
		String out ="";
		
		try(Scanner scan=new Scanner(new URL("https://harri-test-api.herokuapp.com/countries/all").openStream(), "UTF-8")) {
			out=scan.useDelimiter("\\A").next();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		
		if (out!=null&&!out.equals(""))
			jsonStr=out;
		
		List<String> jsonData = Arrays.asList(jsonStr);
		

		Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset.toJavaRDD());
		anotherPeople.select("countryContinent","code").show();
		


		
	}

	
	
	
	private static void testharri6_sql() {
		SparkSession spark=SparkSession.builder().appName("harri-bigdata-test").master("local[*]")
				.getOrCreate();

		
		/**
		 * get countries
		 */
		String jsonStr="";
		try(Scanner scan=new Scanner(new URL("https://harri-test-api.herokuapp.com/countries/all").openStream(), "UTF-8")) {
			jsonStr=scan.useDelimiter("\\A").next();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		

		
		List<String> jsonData = Arrays.asList(jsonStr);
		

		Dataset<String> baseds = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> countriesds = spark.read().json(baseds.toJavaRDD());
		countriesds.createOrReplaceTempView("countries");
		
		countriesds.show();
		/**
		 * END of get countries
		 */
		
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/harri/FIFA-18-Video-Game-Player-Stats-2.csv");
		
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
			  dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
			  
		  });
		  
        
		
		
		
		
		
		
		


	}

	private static void testharri7_read_previous_csv_output() {
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/harri/eu.csv");
		
		dataset.createOrReplaceTempView("fifa");
		
		Dataset<Row> maxds=spark.sql("select * from fifa");
		
		maxds.show();

		
	}	
		

	private static void testharri8_output3_part_1() {
		/**
		 * //1. Which top 3 countries that achieve the highest income through their players?
		 */
		
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
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
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv("src/main/resources/harri/"+c+".csv");
				  //dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
					dataset2.createOrReplaceTempView("fifa"+c);
					Dataset<Row> maxds2=spark.sql("select country_name,sum(salary_n) as total_value from fifa"+c+" group by country_name order by total_value desc limit 3 ");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(java.lang.UnsupportedOperationException e) {
					  System.out.println("Empty collection :-> "+ c);
				  }//

		}

		  maxds.createOrReplaceTempView("fifa_all");
		  maxds.show();
		  Dataset<Row> maxall=spark.sql("select country_name,total_value from fifa_all order by total_value desc limit 3 ");
		  maxall.show();
	
	}	


	private static void testharri9_output3_part_2() {
		/**
		 * //2. List The club that includes the most valuable players, the top 5 clubs that spends highest salaries
		 */
		
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
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
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv("src/main/resources/harri/"+c+".csv");
				  //dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
					dataset2.createOrReplaceTempView("fifa"+c);
					Dataset<Row> maxds2=spark.sql("select Club,sum(salary_n) as total_value from fifa"+c+" group by Club order by total_value desc limit 5 ");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(java.lang.UnsupportedOperationException e) {
					  System.out.println("Empty collection :-> "+ c);
				  }//

		}

		  maxds.createOrReplaceTempView("fifa_all_clubs");
		  maxds.show();
		  Dataset<Row> maxall=spark.sql("select Club,total_value from fifa_all_clubs order by total_value desc limit 5 ");
		  maxall.show();
	
	}	
	
	private static void testharri10_output3_part_3_America_or_Euorpe() {
		/**
		 * //3. Which of Europe or America - on continent level - has the best FIFA players?Note: Elaborate More about how your query is optimized to answer the questions above. Feel
          	free to structure the table/s as you see is better for the performance.

		 */
		
		SparkSession spark=SparkSession.builder().appName("testingSQL").master("local[*]")
				.getOrCreate();


		List<String> cnts=Arrays.asList(
		  "EU",
		  "NA",
		  "SA");


		int counter=0;
		Dataset<Row> maxds = null;
		for (String c:cnts) {
			  try {
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv("src/main/resources/harri/"+c+".csv");
				  //dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
					dataset2.createOrReplaceTempView("fifa"+c);
					Dataset<Row> maxds2=spark.sql("select continent,sum(value_n) as total_value from fifa"+c+" group by continent  ");
					
					if(counter==0) {counter++; maxds=maxds2;continue;}
					maxds=maxds.union(maxds2);
					
				  }catch(java.lang.UnsupportedOperationException e) {
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
	
	}	


	private static void testharri11_write_to_amazon() {
		SparkSession spark=SparkSession.builder().appName("harri-bigdata-test")
				.getOrCreate();

		
		/**
		 * get countries
		 */
		String jsonStr="";
		try(Scanner scan=new Scanner(new URL("https://harri-test-api.herokuapp.com/countries/all").openStream(), "UTF-8")) {
			jsonStr=scan.useDelimiter("\\A").next();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		

		
		List<String> jsonData = Arrays.asList(jsonStr);
		

		Dataset<String> baseds = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> countriesds = spark.read().json(baseds.toJavaRDD());
		countriesds.createOrReplaceTempView("countries");
		
		countriesds.show();
		/**
		 * END of get countries
		 */
		
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("s3://harri-test-bigdata/FIFA-18-Video-Game-Player-Stats-2.csv");
		
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
			  dspercontinent.write().option("header", true).csv("s3://harri-test-bigdata/"+c+".csv");
			  
		  });

	}


private static void testharri12_write_part3_to_amazon_parquet() {
		/**
		 * //1. Which top 3 countries that achieve the highest income through their players?
		 */
		
		SparkSession spark=SparkSession.builder().appName("harri-app-part3")
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
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv("s3://harri-test-bigdata/"+c+".csv");
				  //dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
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
		  maxall.write().option("header", true).parquet("s3://harri-test-bigdata/hadoop-part3/countries_h_income.parquet");
	
		/**
		 * //2. List The club that includes the most valuable players, the top 5 clubs that spends highest salaries
		 */


		 counter=0;
		 maxds = null;
		for (String c:cnts) {
			  try {
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv("s3://harri-test-bigdata/"+c+".csv");
				  //dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
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
		  maxall.write().option("header", true).parquet("s3://harri-test-bigdata/hadoop-part3/club_h_valuable_players.parquet");

		/**
		 * //3. Which of Europe or America - on continent level - has the best FIFA players?Note: Elaborate More about how your query is optimized to answer the questions above. Feel
          	free to structure the table/s as you see is better for the performance.

		 */

		 counter=0;
		 maxds = null;
		for (String c:cnts) {
			  try {
				  Dataset<Row> dataset2 = spark.read().option("header", true).csv("s3://harri-test-bigdata/"+c+".csv");
				  //dspercontinent.write().option("header", true).csv("src/main/resources/harri/"+c+".csv");
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
		  maxall.write().option("header", true).parquet("s3://harri-test-bigdata/hadoop-part3/europe_or_America_best_players.parquet");
	
		
	
}

static private void test_update_13() {
	SparkSession spark=SparkSession.builder().appName("harri-bigdata-test").master("local[*]")
			.getOrCreate();

	
	/**
	 * get countries
	 */
	String jsonStr="";
	try(Scanner scan=new Scanner(new URL("https://harri-test-api.herokuapp.com/countries/all").openStream(), "UTF-8")) {
		jsonStr=scan.useDelimiter("\\A").next();
	} catch (MalformedURLException e) {
		e.printStackTrace();
	} catch (IOException e) {
		
		e.printStackTrace();
	}
	
	

	
	List<String> jsonData = Arrays.asList(jsonStr);
	

	Dataset<String> baseds = spark.createDataset(jsonData, Encoders.STRING());
	Dataset<Row> countriesds = spark.read().json(baseds.toJavaRDD());
	countriesds.createOrReplaceTempView("countries");
	
	countriesds.show();
	/**
	 * END of get countries
	 */
	
String sourceEnd="src/main/resources/harri/FIFA-18-Video-Game-Player-Stats-2.csv";
//sourceEnd="s3://harri-test-bigdata/FIFA-18-Video-Game-Player-Stats-2.csv";
	Dataset<Row> dataset = spark.read().option("header", true).csv(sourceEnd);
	
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
	
	/*  List<String> cnts=Arrays.asList("AF",
	  "AS",
	  "EU",
	  "NA",
	  "SA",
	  "OC",
	  "AN");
	  */

	  List<String> cnts=Arrays.asList("AF");
	  final Dataset<Row> dataresult=alljoin;
	  cnts.forEach((c) -> {
		  Dataset<Row> dspercontinent=dataresult.filter("continent='"+c+"'");
		  dspercontinent.createOrReplaceTempView("fifa_new"+c);
		  /*ingest upsert*/
		  /*=====================================================================*/
		  String targetDest="src/main/resources/harri/"+c+".csv";
		//  	targetDest="s3://harri-test-bigdata/"+c+".csv";
		   Dataset<Row> dataset2 = spark.read().option("header", true).csv(targetDest);
		  Map<String, String> hudiOptions =new  Hashtable<>();
		  		   //hudiOptions.put(DataSourceWriteOptions.TABLE_NAME().toString(), "fifa_players"+c);
	  			   hudiOptions.put(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(),"COPY_ON_WRITE");
				   hudiOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(),"id");
				   hudiOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "creation_date" );
				   hudiOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY() ,"last_update_time" );
				   hudiOptions.put(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY() , "true"            );
				   hudiOptions.put(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY() , "fifa_players"+c);
				   hudiOptions.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY() , "creation_date");
				   hudiOptions.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY() , MultiPartKeysValueExtractor.class.getName());
		String writeFilePath="s3://DOC-EXAMPLE-BUCKET/s3://harri-test-bigdata/upsert_ds_husdi/";
		writeFilePath="src/main/resources/harri/";
		   dataset2.write()
		   .format("org.apache.hudi")
		   .options(hudiOptions)
/*		   .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())

		   .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(),"COPY_ON_WRITE")
		   .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(),"id")
		   .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "creation_date" )
		   .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY() ,"last_update_time" )
		   .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY() , "true"            )
		   .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY() , "fifa_players"+c)
		   .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY() , "creation_date")
		   .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY() , MultiPartKeysValueExtractor.class.getName())
		   */
		   .option("hoodie.table.name","fifa_players"+c)
		   .mode(SaveMode.Overwrite)
		   .save(writeFilePath);
			  
			
		  /*=====================================================================*/
		  /*ingest upsert*/
			
		 
		  
	  });
	  

	
}

}