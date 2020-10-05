package com.hash.p79.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.transaction.annotation.Transactional;

import com.hash.p79.domain.EtlEligibility;

@Mapper
public interface EtlEligibilityMapper {
	
	@Select("select max(ele.row_num) as numberOfEligRecords from etl_l_eligibility ele where ele.job_id = #{jobid}")
	public int numberOfEligRecords(Long jobid);
	
	public List<EtlEligibility> eligRecords(@Param("jobid") Long jobid,  @Param("low") int low, 
									@Param("high")int high);
	
	public List<EtlEligibility> getMedicaidEligibleCases(Long jobId);
	
	
	

}
