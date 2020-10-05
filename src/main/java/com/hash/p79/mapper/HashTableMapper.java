package com.hash.p79.mapper;


import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Mapper
public interface HashTableMapper {
	
	@Insert("insert into HashTable(id, clientnbr, hashmessage, disguise, jobid) values(seq_hashtable.nextval, #{clientnbr}, #{hashmessage}, #{disguise, jdbcType=VARCHAR}, #{jobid}) ")
	public void insertHashWithDisguiseClient(@Param("clientnbr") String clientnbr, @Param("hashmessage") String hashmessage, @Param("disguise") String disguise, @Param("jobid") Long jobid);
	
	@Insert("insert into HashTable(id, clientnbr, hashmessage, jobid) values(seq_hashtable.nextval, #{clientnbr}, #{hashmessage}, #{jobid}) ")
	public void insertHash(@Param("clientnbr") String clientnbr, @Param("hashmessage") String hashmessage, @Param("jobid") Long jobid);

}
