package com.hash.p79.dao;

import java.util.List;
import java.util.Map;

public interface HashTableDao {
	
	public void insertEtlEligRecords(Map<String, String> uniqueclientsforhash, Map<String, List<String>> multipleclientsforhash, Long jobid);

}
