package dev.harritest.model.impl.merge;

import dev.harritest.model.AppConf;

public class DetectMergeFile {

/**
 * right now it is just trivial implementation 
 * need to detect new files and lock one for processing by renaming or something   
 * @return locked file for processing
 */
	public static String lockone() {
		return AppConf.LOCAL_SOURCE_PATH;
	}

	/**
	 * TODO, renamed as processed
	 * move to processed location or any other implementation mechanism
	 * @return
	 */
	public static boolean processLocked() {
		
		return true;
		
	}

}
