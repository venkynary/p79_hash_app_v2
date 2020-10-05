package com.hash.p79.application;

public class EligRecordHash {
	
	private String hash;
	private String clientNbr;
	
	public EligRecordHash(String clientNbr, String hash) {
		this.clientNbr = clientNbr;
		this.hash = hash;
	}
	
	public String getHash() {
		return hash;
	}

	public String getClientNbr() {
		return clientNbr;
	}

	@Override
	public int hashCode() {
		return hash.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EligRecordHash other = (EligRecordHash) obj;
		if (clientNbr == null) {
			if (other.clientNbr != null)
				return false;
		} else if (!clientNbr.equals(other.clientNbr))
			return false;
		return true;
	}

	
	
	
	
	
	
	

}
