package com.hash.p79.application;

public abstract class P79ShaDigest implements EbMessageDigest {
	
	protected abstract String digestMessage(String message) throws Exception;

	@Override
	public String digest(String message) {
		try {
			return digestMessage(message);
		}catch(Exception cause){
			throw new RuntimeException("an exception occured in digest", cause);
		}
	}
	
	protected String bytesToHex(byte[] hash){
		StringBuffer hexString = new StringBuffer();
	    
		for (int i = 0; i < hash.length; i++) {
			
			String hex = Integer.toHexString(0xff & hash[i]);
	    
			if(hex.length() == 1) hexString.append('0');
	    
			hexString.append(hex);
	    }
		
	    return hexString.toString();
	}
	
	

}
