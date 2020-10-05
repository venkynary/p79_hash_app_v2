package com.hash.p79.application;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientHashHolder {
	
	private Map<String, String> clientforhash = Collections.synchronizedMap(new HashMap<String, String>());
	private Map<String, List<String>> multipleclientsforhash  = Collections.synchronizedMap(new HashMap<String, List<String>>());
	
	public void put(String hashmessage, String clientnbr){
		
		if(! clientforhash.containsKey(hashmessage)) {
			clientforhash.put(hashmessage, clientnbr);
		} else {
			
			if(! multipleclientsforhash.containsKey(hashmessage)) {
				
				String otherclientnbr = clientforhash.get(hashmessage);
				
				List<String> clientnbrs = new ArrayList<>();
				clientnbrs.add(otherclientnbr);
				clientnbrs.add(clientnbr);
				multipleclientsforhash.put(hashmessage, clientnbrs);
			} else {
				multipleclientsforhash.get(hashmessage).add(clientnbr);
			}
			
		}
		
	}
	
	public int getsizeofuniquehashclientmap(){
		return clientforhash.size();
	}
	
	public int getsizeofduplicatehashclientmap(){
		return multipleclientsforhash.size();
	}

	public Map<String, String> getClientforhash() {
		return clientforhash;
	}

	public Map<String, List<String>> getMultipleclientsforhash() {
		return multipleclientsforhash;
	}
	
	

}
