package com.hash.p79.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.hash.p79.application.ClientHashHolder;
import com.hash.p79.application.P79ShaDigest;
import com.hash.p79.application.Sha1Digest;
import com.hash.p79.application.Sha256Digest;
import com.hash.p79.enums.HashTypes;
import com.hash.p79.mapper.EtlEligibilityMapper;
import com.hash.p79.mapper.HashTableMapper;
import com.hash.p79.thread.tasks.ClientHashTask;
import com.hash.p79.thread.tasks.InsertHashTask;


public class EtlEligibilityServiceImpl implements EtlEligibilityService{
	
	private static final Logger logger = LoggerFactory.getLogger(EtlEligibilityServiceImpl.class);
	
	private EtlEligibilityMapper etlEligibilityMapper;
	
	private HashTableMapper hashTableMapper;
	
	private ClientHashHolder clientHashHolder;
	
	public EtlEligibilityServiceImpl(EtlEligibilityMapper etlEligibilityMapper, HashTableMapper hashTableMapper,
					ClientHashHolder clientHashHolder){
		this.etlEligibilityMapper = etlEligibilityMapper;
		this.hashTableMapper = hashTableMapper;
		this.clientHashHolder = clientHashHolder;
	}
	
	@Override
	public void processEtlEligRecords(Long jobId, String hashType, Integer taskcount, Integer querybatchsize){
		
		logger.info("processing EtlElig records for job id = " + jobId + ", with hashType = " + hashType);
		
		P79ShaDigest messagedigest = null;
		
		if(HashTypes.SHA_256.toString().equals(hashType)){
			messagedigest = new Sha256Digest();
		} else if(HashTypes.SHA_1.toString().equals(hashType)){
			messagedigest = new Sha1Digest();
		}
		
		Assert.notNull(messagedigest, "cannot be null");
		
		processEtlEligibility(jobId, taskcount, querybatchsize, messagedigest);
		
		processHashTable(jobId, taskcount);
		
		
		
	}

	private void processEtlEligibility(Long jobId, Integer taskcount, Integer querybatchsize,
			P79ShaDigest messagedigest) {
		
		int totalnumberofeligrecords = etlEligibilityMapper.numberOfEligRecords(jobId);
		
		logger.info("the total number of the elig records are " + totalnumberofeligrecords);
		
		ExecutorService executor = Executors.newFixedThreadPool(taskcount);
		
		List<ClientHashTask> callabletasks = new ArrayList<>(taskcount);
		
		int lowestnumberofrecord = 0;
		
		int eligrecordsfortask = totalnumberofeligrecords/taskcount;
		
		logger.info("the elig records for each task to process are " + eligrecordsfortask);
		
		int remaindereligrecords = totalnumberofeligrecords % taskcount;
		
		logger.info("the remaining record count is " + remaindereligrecords);
		
		for(int i = 1; i <= taskcount; i ++){
			
			if(i == taskcount) eligrecordsfortask += remaindereligrecords; //last thread could have few more records to process.
				
			callabletasks.add(new ClientHashTask("ClientHash task " + i, jobId, lowestnumberofrecord, 
					querybatchsize, eligrecordsfortask, 
					etlEligibilityMapper, messagedigest, 
					clientHashHolder));
			
			lowestnumberofrecord += eligrecordsfortask;
			
		}
		
		logger.info("the number of the tasks created are " + callabletasks.size());
		
		try {
			
			executor.invokeAll(callabletasks);

			executor.shutdown();

		}catch(InterruptedException ie){
			logger.info("the exception is " + ie.getMessage());
			executor.shutdown();
			try {
				if(executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
					executor.shutdownNow();
				}
			}catch(InterruptedException ie2){
				logger.info("the exception is " + ie2.getMessage());
				executor.shutdownNow();
			}
		
		}
	}

	private void processHashTable(Long jobId, Integer taskcount) {
		logger.info("inserting hashes into the hash table");
		
		List<Entry<String, String>> clientforhashentrylist= clientHashHolder.getClientforhash().entrySet()
																.stream()
																.collect(Collectors.toCollection(ArrayList::new));
		
		List<Entry<String, List<String>>> multipleclientsforhashentrylist = clientHashHolder.getMultipleclientsforhash().entrySet()
																				.stream()
																				.collect(Collectors.toCollection(ArrayList::new));
		
		logger.info("the total client hash entries are :" + clientforhashentrylist.size());
		
		int clienthashentriesfortask = clientforhashentrylist.size()/taskcount;
		
		logger.info("the client for hash entries for each task are " + clienthashentriesfortask);
		
		int remainderclienthashentries = clientforhashentrylist.size() % taskcount;
		
		logger.info("the remainder client hash entries that needs an assign are " + remainderclienthashentries);
		
		logger.info("the total multiple client for hash entries are :" + multipleclientsforhashentrylist.size());
		
		int multipleclienthashentriesfortask = multipleclientsforhashentrylist.size()/taskcount;
		
		logger.info("the multiple clients for hash for each task are " + multipleclienthashentriesfortask);
		
		int remaindermultipleclienthashentries = multipleclientsforhashentrylist.size() % taskcount;
		
		logger.info("the remainder mutliple client hash entries that needs an assign are " + remaindermultipleclienthashentries);
		
		ExecutorService inserthashexecutor = Executors.newFixedThreadPool(taskcount);
		
		List<InsertHashTask> inserthashtasks = new ArrayList<>(taskcount);
		
		int lowerClienthashBound = 0;
		int higherClienthashBound = 0;
		
		int lowerMultipleClienthashBound = 0;
		int higherMultipleClienthashBound = 0;
		
		
		for(int i = 0; i < taskcount; i++) {
			
			if(i == taskcount - 1) {
				clienthashentriesfortask += remainderclienthashentries;
				multipleclienthashentriesfortask += remaindermultipleclienthashentries;
			}
			
			higherClienthashBound += clienthashentriesfortask;
			higherMultipleClienthashBound += multipleclienthashentriesfortask;
			
			if(higherClienthashBound > clientforhashentrylist.size()) higherClienthashBound = clientforhashentrylist.size();
			
			if(higherMultipleClienthashBound > multipleclientsforhashentrylist.size()) higherMultipleClienthashBound = multipleclientsforhashentrylist.size();
			
			inserthashtasks.add(new InsertHashTask("InsertHash task" + (i+1), hashTableMapper, 
										clientforhashentrylist.subList(lowerClienthashBound, higherClienthashBound), 
										multipleclientsforhashentrylist.subList(lowerMultipleClienthashBound, higherMultipleClienthashBound), 
										jobId));
			
			
			lowerClienthashBound = higherClienthashBound;
			lowerMultipleClienthashBound = higherMultipleClienthashBound;
			
		}
		
		logger.info("the number of the inserthash tasks created are " + inserthashtasks.size());

		
		try {
			
			inserthashexecutor.invokeAll(inserthashtasks);

			inserthashexecutor.shutdown();

		}catch(InterruptedException ie){
			logger.info("the exception is " + ie.getMessage());
			inserthashexecutor.shutdown();
			try {
				if(inserthashexecutor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
					inserthashexecutor.shutdownNow();
				}
			}catch(InterruptedException ie2){
				logger.info("the exception is " + ie2.getMessage());
				inserthashexecutor.shutdownNow();
			}
		
		}
	}

	
}
