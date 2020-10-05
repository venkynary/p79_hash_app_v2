package com.hash.p79.thread.tasks;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.ibatis.executor.statement.CallableStatementHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import com.hash.p79.application.ClientHashHolder;
import com.hash.p79.application.EbMessageDigest;
import com.hash.p79.domain.EtlEligibility;
import com.hash.p79.mapper.EtlEligibilityMapper;
import com.hash.p79.mapper.HashTableMapper;

public class ClientHashTask implements Callable<String> {
	
	private static final Logger logger = LoggerFactory.getLogger(ClientHashTask.class);
	
	private String name;

	private Long jobid;
	
	private int lowestnumberofrecord;
	private int highestnumberofrecord;
	private int maximumrecordsthatcanbeprocessed;
	private int numberofrecordsyettobeprocessed;
	
	private EbMessageDigest messageDigest;
	private EtlEligibilityMapper etlEligibilityMapper;
	private ClientHashHolder clientHashHolder;
	
	public ClientHashTask(String name, Long jobid, int lowestnumberofrecord,
				int maximumrecordsthatcanbeprocessed, int numberofrecordsyettobeprocessed,
				EtlEligibilityMapper etlEligibilityMapper, EbMessageDigest messageDigest,
				ClientHashHolder clientHashHolder) {
		
		this.name = name;
		this.jobid = jobid;
		this.lowestnumberofrecord = lowestnumberofrecord;
		this.highestnumberofrecord = lowestnumberofrecord;
		this.maximumrecordsthatcanbeprocessed = maximumrecordsthatcanbeprocessed;
		this.numberofrecordsyettobeprocessed = numberofrecordsyettobeprocessed;
		
		this.etlEligibilityMapper = etlEligibilityMapper;
		this.messageDigest = messageDigest;
		this.clientHashHolder = clientHashHolder;
	}
	
	

	@Override
	public String call() throws Exception {
		
		StopWatch mywatch = new StopWatch();
		mywatch.start();
		
		logger.info(String.format("the task [%s] has started", name));
		
		do {
			
			lowestnumberofrecord = highestnumberofrecord;
			
			if(numberofrecordsyettobeprocessed <= maximumrecordsthatcanbeprocessed) {
				highestnumberofrecord = (lowestnumberofrecord + numberofrecordsyettobeprocessed);
			}
			else {
				highestnumberofrecord = (lowestnumberofrecord + maximumrecordsthatcanbeprocessed);
			}
			numberofrecordsyettobeprocessed = numberofrecordsyettobeprocessed - maximumrecordsthatcanbeprocessed; // its ok for the value of numberofrecordsyettobeprocessed 
																												  // to become negative
			
			StopWatch recordsretrievewatch = new StopWatch();
			recordsretrievewatch.start();
			
			logger.info(String.format("the task [%s] for retrieving the records from [%d] to [%d] ", name, 
							lowestnumberofrecord, highestnumberofrecord));
			
			List<EtlEligibility> eligrecords = etlEligibilityMapper.eligRecords(jobid, lowestnumberofrecord, highestnumberofrecord);
			
			recordsretrievewatch.stop();
			
			logger.info(String.format("the task [%s] took [%d] seconds for retrieving the records from [%d] to [%d] ", name, 
					recordsretrievewatch.getTime(TimeUnit.SECONDS), lowestnumberofrecord, highestnumberofrecord));
			
			StopWatch processrecordswatch = new StopWatch();
			processrecordswatch.start();
			
			for(EtlEligibility curreligrecord : eligrecords) {
				
				logger.debug("the record in string " + curreligrecord);
				
				String clientnbr = curreligrecord.getClientNbr();
				
				String hashmessage = messageDigest.digest(curreligrecord.toString());
				
				logger.debug("the hash message for the record is " + hashmessage);
				
				clientHashHolder.put(hashmessage, clientnbr);
				
			}
			
			processrecordswatch.stop();
			
			logger.info(String.format("the task [%s] tooks [%d] seconds to processing the records into the map", name, processrecordswatch.getTime(TimeUnit.SECONDS)));
			

			
		} while(numberofrecordsyettobeprocessed > 0);
		
		mywatch.stop();
		
		String result = String.format("the task [%s] took [%d] second to complete", name, mywatch.getTime(TimeUnit.SECONDS));
		
		logger.info(result);
		
		return result;
		
	}

}
