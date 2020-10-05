package com.hash.p79.service;

public interface EtlEligibilityService {
	
	public void processEtlEligRecords(Long jobId, String hashType, Integer threadcount, Integer querybatchsize);

}
