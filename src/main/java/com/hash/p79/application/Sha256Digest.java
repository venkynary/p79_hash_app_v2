package com.hash.p79.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;


public class Sha256Digest extends P79ShaDigest {

	@Override
	public String digestMessage(String message) throws Exception{
		
		MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
		
		byte[] encodedHash = messageDigest.digest(message.getBytes(StandardCharsets.UTF_8));
		
		return bytesToHex(encodedHash);
		
	}
	
	
	
	
}
