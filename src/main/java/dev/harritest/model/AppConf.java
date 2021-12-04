package dev.harritest.model;

public class AppConf {
	/**
	 *  put it true for the first run
	 */
	public static final boolean RUN_INITIALIZE=false;
	/**
	 * true for local and false for cloud
	 */
	public static final boolean RUN_LOCAL=true;
	/**
	 * hudi or ugly
	 */
	public static final String MERGE_METHOD="hudi";  
	public static final String APP_NAME="harri-bigdata-test";
	public static final String  LOCAL_SOURCE_PATH="src/main/resources/harri/FIFA-18-Video-Game-Player-Stats-2.csv";
	public static final String LOCAL_DEST_INITIALIZE_PATH= "src/main/resources/harri/";
	public static final String CLOUD_SOURCE_PATH="s3://harri-test-bigdata/FIFA-18-Video-Game-Player-Stats-2.csv";
	public static final String CLOUD_DEST_INITIALIZE_PATH="s3://harri-test-bigdata/";
	public static final String LOCAL_DEST_ANALYSIS_PATH="src/main/resources/harri/hadoop-part3/";
	public static final String CLOUD_DEST_ANALYSIS_PATH="s3://harri-test-bigdata/hadoop-part3/";
	public static final String LOCAL_DEST_PREPARE_ANALYSIS_PATH="src/main/resources/harri/hadoop-part3/";
	public static final String CLOUD_DEST_PREPARE_ANALYSIS_PATH="s3://harri-test-bigdata/hadoop-part3/";
	
	public static final String CLOUD_MERGE_PATH="s3://harri-test-bigdata/all-merge/";
	public static final String LOCAL_MERGE_PATH="file:///./src/main/resources/harri/all-merge/";
	public static final String LOCAL_STAGED_UPDATE_PATH="src/main/resources/harri/staged_update/";
	public static final String CLOUD_STAGED_UPDATE_PATH="s3://harri-test-bigdata//staged_update/";
	
	
}
