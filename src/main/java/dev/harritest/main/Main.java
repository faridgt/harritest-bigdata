package dev.harritest.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import dev.harritest.controller.DataAnalysisPrepareTable;
import dev.harritest.controller.DataAnalysisQueryPersistence;
import dev.harritest.controller.DataHandlerInitializer;
import dev.harritest.controller.DataMergeHandler;
import dev.harritest.controller.HandleInstances;
import dev.harritest.model.AppConf;

public class Main {
public static void main(String[] args) {
	
	Logger.getLogger("org.apache").setLevel(Level.WARN);
	

	Logger.getRootLogger().setLevel(Level.WARN);

	
	/*
	 * run initializer for the first time 
	 */
	if(AppConf.RUN_INITIALIZE) {
		DataHandlerInitializer init=HandleInstances.getInitializerInstanceInstance();
		init.handleInitial();
	}

	
	/*
	 * Run Prepare analysis table
	 */
//	DataAnalysisPrepareTable dataAnalysisPrepareTable=HandleInstances.getCloudDataAnalysisPrepareTableInstance();
//	dataAnalysisPrepareTable.prepareDataTable();

	
	/*
	 * Run Analysis to persist query results 
	 */
//	DataAnalysisQueryPersistence dataAnalysisQueryPersistence=HandleInstances.getDataAnalysisQueryPersistenceInstance();
//	dataAnalysisQueryPersistence.hitAnalysisAndPersist();
	
	
	
	/*
	 * Run merge
	 */
	DataMergeHandler dataMergeHandler=HandleInstances.getDataMergeHandlerInstance();
	dataMergeHandler.mergeUpdateDelete();
	
}
}
