package com.hash.p79.enums;

public enum RunOptions {
	
	HASH_TYPE("hashtype"),
	JOB_ID("jobid"),
	THREADS("noofthreads"),
	QUERY_BATCH_SIZE("querybatchsize"),
	RECORDS_FOR_EACH_THREAD("recordsperthread");
	
	
	
	private String optionname;
	
	private RunOptions(String optionname){
		this.optionname = optionname;
	}

	public String getOptionname() {
		return optionname;
	}
	
	public static boolean isOptionExists(String optionname){
		
		RunOptions[] options =  RunOptions.values();
		
		for(RunOptions option : options ) {
			if(option.optionname.equals(optionname)) return true;
		}
		
		return false;
	}
	
	

}
