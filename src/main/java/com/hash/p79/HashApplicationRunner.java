package com.hash.p79;

import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;

import com.hash.p79.application.ClientHashHolder;
import com.hash.p79.enums.RunOptions;
import com.hash.p79.mapper.EtlEligibilityMapper;
import com.hash.p79.mapper.HashTableMapper;
import com.hash.p79.service.EtlEligibilityService;
import com.hash.p79.service.EtlEligibilityServiceImpl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


@SpringBootApplication
@MapperScan("com.hash.p79.mapper")
public class HashApplicationRunner implements ApplicationRunner {
	
	private static final Logger logger = LoggerFactory.getLogger(HashApplicationRunner.class);
	
	@Bean
	public DataSource dataSource() {
		
		HikariConfig hikariconfig = new HikariConfig();
		
		hikariconfig.setJdbcUrl("jdbc:oracle:thin:@ldohstxeb004.oracleoutsourcing.com:30210:RTXEBT");
		hikariconfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		hikariconfig.setUsername("EBTRANSAC");
		hikariconfig.setPassword("N30rn8u5k5rs");
		
		hikariconfig.setConnectionTimeout(60 * 60 * 1000);
		hikariconfig.setIdleTimeout(60 * 60 * 1000);
		hikariconfig.setMaxLifetime(60 * 60 * 1000);
		hikariconfig.setMaximumPoolSize(40);

		return new HikariDataSource(hikariconfig);
	}
	
	@Bean
	public PlatformTransactionManager transactionalManager() {
		return new DataSourceTransactionManager(dataSource());
	}
	
	@Bean
	public HashTableMapper getHashTableMapper() throws Exception {
		MapperFactoryBean<HashTableMapper> mapperfactorybean = new MapperFactoryBean<>();
		mapperfactorybean.setMapperInterface(HashTableMapper.class);
		mapperfactorybean.setSqlSessionTemplate(getBatchSqlSessionTemplate());
		return mapperfactorybean.getObject();
	}

	@Bean
	public EtlEligibilityMapper getEtlEligibilityMapper() throws Exception{
		return getBatchSqlSessionTemplate().getMapper(EtlEligibilityMapper.class);
	}
	
	@Bean
	public ClientHashHolder getClientHashHolder() {
		return new ClientHashHolder();
	}
	
	@Bean
	public SqlSessionFactory getSqlSessionFactory() throws Exception {
		SqlSessionFactoryBean sessionfactorybean = new SqlSessionFactoryBean();
		sessionfactorybean.setDataSource(dataSource());
		sessionfactorybean.setMapperLocations(new ClassPathResource("com/hash/p79/mapper/EtlEligibilityMapper.xml"),
											  new ClassPathResource("com/hash/p79/mapper/HashTableMapper.xml"));
		
		return sessionfactorybean.getObject();
	}
	
	@Bean
	public SqlSessionTemplate getBatchSqlSessionTemplate() throws Exception {
		SqlSessionTemplate batchsessiontemplate = new SqlSessionTemplate(getSqlSessionFactory(), ExecutorType.BATCH);
		return batchsessiontemplate;
	}
	
	@Bean
	public EtlEligibilityService getEtlEligibilityService() throws Exception {
		return new EtlEligibilityServiceImpl(getEtlEligibilityMapper(), getHashTableMapper(), getClientHashHolder());
		
	}

	
	
	private static final Integer DEFAULT_THREAD_COUNT = 10; 
	private static final Integer DEFAULT_QUERY_BATCH_SIZE = 20000;
	
	public static void main(String[] args) {
		
		logger.info("enter the main method");
		
        for(String arg:args) {
            logger.debug("the passed arguments are " + arg);
        }
		
		SpringApplication.run(HashApplicationRunner.class, args);
		
		logger.info("exit the main method");
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		logger.info("entered the run method");
		
		Set<String> options = args.getOptionNames();
		
		for(String option : options){
			if(!RunOptions.isOptionExists(option)){
				logger.info("usage: --hashtype=#hashtype --jobid=#jobid");
			}
		}
		
		List<String> hashtypes = args.getOptionValues("hashtype");
		List<String> jobids = args.getOptionValues("jobid");
		List<String> noofthreads = args.getOptionValues("noofthreads");
		List<String> querybatchsize = args.getOptionValues("querybatchsize");
		
		
		
		org.springframework.util.Assert.notEmpty(hashtypes, "hashtypes cannot be empty");
		org.springframework.util.Assert.notEmpty(jobids, "jobids cannot be empty");
		
		String hashtype = hashtypes.get(0);
		Long jobid = Long.valueOf(jobids.get(0));
		
		Integer threadcount = 0;
		if(CollectionUtils.isEmpty(noofthreads)){
			threadcount = DEFAULT_THREAD_COUNT;
		} else {
			threadcount = Integer.valueOf(noofthreads.get(0));
		}
		
		Integer qbs = 0;
		if(CollectionUtils.isEmpty(querybatchsize)){
			qbs = DEFAULT_QUERY_BATCH_SIZE;
		} else {
			qbs = Integer.valueOf(querybatchsize.get(0));
		}
		
					
		
		getEtlEligibilityService().processEtlEligRecords(jobid, hashtype, threadcount, qbs);
		
	}

	
	

}
