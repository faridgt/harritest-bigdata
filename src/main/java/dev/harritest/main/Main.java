package dev.harritest.main;

import dev.harritest.controller.DataAnalysisQueryPersistence;
import dev.harritest.controller.DataHandlerInitializer;
import dev.harritest.controller.DataMergeHandler;
import dev.harritest.controller.HandleInstances;
import dev.harritest.model.AppConf;

public class Main {
public static void main(String[] args) {
	
	/*
	 * run initializer for the first time 
	 */
	if(AppConf.RUN_INITIALIZE) {
		DataHandlerInitializer init=HandleInstances.getInitializerInstanceInstance();
		init.handleInitial();
	}
	
	/*
	 * Run Analysis to persist query results 
	 */
	DataAnalysisQueryPersistence dataAnalysisQueryPersistence=HandleInstances.getDataAnalysisQueryPersistenceInstance();
	dataAnalysisQueryPersistence.hitAnalysisAndPersist();
	
	
	/**
	 * Run merge
	 */
	DataMergeHandler dataMergeHandler=HandleInstances.getDataMergeHandlerInstance();
	dataMergeHandler.mergeUpdateDelete();
	
}
}
