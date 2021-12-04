package dev.harritest.controller;

import dev.harritest.model.AppConf;
import dev.harritest.model.impl.CloudDataAnalysisPrepareTable;
import dev.harritest.model.impl.CloudDataAnalysisQueryPersistence;
import dev.harritest.model.impl.CloudDataHandlerInitializer;
import dev.harritest.model.impl.DefaultContinentJSONSourceImpl;
import dev.harritest.model.impl.LocalDataAnalysisPrepareTable;
import dev.harritest.model.impl.LocalDataAnalysisQueryPersistence;
import dev.harritest.model.impl.LocalDataHandlerInitializer;
import dev.harritest.model.impl.merge.CloudHudiDataMergeHandler;
import dev.harritest.model.impl.merge.CloudUglyDataMergeHandler;
import dev.harritest.model.impl.merge.LocalHudiDataMergeHandler;
import dev.harritest.model.impl.merge.LocalUglyDataMergeHandler;

public class HandleInstances {
	private static  ContinentJSONSource continentJSONSourceInst;
	private static DataHandlerInitializer dataHandlerInitializer;
	private static DataAnalysisQueryPersistence dataAnalysisQueryPersistence;
	private static DataMergeHandler dataMergeHandler;
	private static DataAnalysisPrepareTable dataAnalysisPrepareTable;

	
public static ContinentJSONSource getOrCreateContinentJSONSourceInstance() {
	
	if(continentJSONSourceInst!=null)
		return continentJSONSourceInst;
	
	continentJSONSourceInst=new DefaultContinentJSONSourceImpl();
	
	return continentJSONSourceInst;
}

public static DataHandlerInitializer getInitializerInstanceInstance() {
	
	if(dataHandlerInitializer!=null) return dataHandlerInitializer;
	if(AppConf.RUN_LOCAL)
		dataHandlerInitializer=new LocalDataHandlerInitializer(); 
	else
		dataHandlerInitializer=new CloudDataHandlerInitializer();
	
	return dataHandlerInitializer;

}



public static DataAnalysisQueryPersistence getDataAnalysisQueryPersistenceInstance() {
	
	if(dataAnalysisQueryPersistence!=null) return dataAnalysisQueryPersistence;
	if(AppConf.RUN_LOCAL)
		dataAnalysisQueryPersistence=new LocalDataAnalysisQueryPersistence(); 
	else
		dataAnalysisQueryPersistence=new CloudDataAnalysisQueryPersistence(); 
	
	return dataAnalysisQueryPersistence;

}

public static DataMergeHandler getDataMergeHandlerInstance() {
	
	if(dataMergeHandler!=null) return dataMergeHandler;
	if(AppConf.RUN_LOCAL)
		if(AppConf.MERGE_METHOD.equals("hudi"))
			dataMergeHandler=new LocalHudiDataMergeHandler();
		else
			dataMergeHandler=new LocalUglyDataMergeHandler();
	else
		if(AppConf.MERGE_METHOD.equals("hudi"))
			dataMergeHandler=new CloudHudiDataMergeHandler();
		else
			dataMergeHandler=new CloudUglyDataMergeHandler();
	
	return dataMergeHandler;

}


public static DataAnalysisPrepareTable getCloudDataAnalysisPrepareTableInstance() {
	
	if(dataAnalysisPrepareTable!=null) return dataAnalysisPrepareTable;
	if(AppConf.RUN_LOCAL)
		dataAnalysisPrepareTable=new LocalDataAnalysisPrepareTable(); 
	else
		dataAnalysisPrepareTable=new CloudDataAnalysisPrepareTable(); 
	
	return dataAnalysisPrepareTable;

}


}
