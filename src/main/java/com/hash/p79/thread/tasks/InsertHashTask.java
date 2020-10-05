package com.hash.p79.thread.tasks;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hash.p79.mapper.HashTableMapper;

public class InsertHashTask implements Callable<String>{
	
	private static final Logger logger = LoggerFactory.getLogger(ClientHashTask.class);
	
	private String name;
	
	private HashTableMapper hashTableMapper;
	
	private List<Entry<String, String>> clientforhash;
	
	private List<Entry<String, List<String>>> multipleclientsforhash;
	
	private Long jobid;
	
	public InsertHashTask(String name, HashTableMapper hashTableMapper, 
			List<Entry<String, String>> clientforhash,
			List<Entry<String, List<String>>> multipleclientsforhash,
			Long jobid){
		
		this.name = name;
		this.jobid = jobid;
		this.hashTableMapper = hashTableMapper;
		this.clientforhash = clientforhash;
		this.multipleclientsforhash = multipleclientsforhash;
		
	}
	
	@Override
	public String call() throws Exception {
		// TODO Auto-generated method stub
		
		logger.info(String.format("the [%d] clienthash records, [%d] multipleclientsforhash records for task [%s]", clientforhash.size(), 
				multipleclientsforhash.size(), name));
		
		StopWatch totalwatch = new StopWatch();
		totalwatch.start();
		
		StopWatch clienthashwatch = new StopWatch();
		clienthashwatch.start();
		
		for(Entry<String, String> clienthashentry : clientforhash){
			hashTableMapper.insertHash(clienthashentry.getValue(), clienthashentry.getKey(), jobid);
		}
		
		clienthashwatch.stop();
		
		logger.info(String.format("the time [%d] seconds taken to process [%d] clienthash records ", clienthashwatch.getTime(TimeUnit.SECONDS)), clientforhash.size());
		
		for(Entry<String, List<String>> multipleclienthashentry : multipleclientsforhash){
			
			List<String> multipleclientnumbers = multipleclienthashentry.getValue();
			
			String clientnumber = multipleclientnumbers.get(0); // get the index 0 client nbr;
			
			for(int i = 1; i < multipleclientnumbers.size(); i++){ // insert other indexes as disguise clintnbrs to index 0 clientnbr.
				
				hashTableMapper.insertHashWithDisguiseClient(clientnumber, 
						multipleclienthashentry.getKey(),multipleclientnumbers.get(i), jobid);
				
			}
			
		}
		
		totalwatch.stop();
		
		String result = String.format("the total time [%d] seconds taken to process the [%d] clienthash and [%d] multipleclientforhash records", 
				totalwatch.getTime(TimeUnit.SECONDS), clientforhash.size(), multipleclientsforhash.size());
		
		logger.info(result);
		
		return result;
		
	}
	
	public String getName() {
		return name;
	}

	public HashTableMapper getHashTableMapper() {
		return hashTableMapper;
	}

	public List<Entry<String, String>> getClientforhash() {
		return clientforhash;
	}

	public List<Entry<String, List<String>>> getMultipleclientsforhash() {
		return multipleclientsforhash;
	}

}
