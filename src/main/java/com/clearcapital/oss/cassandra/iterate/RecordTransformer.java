package com.clearcapital.oss.cassandra.iterate;

import com.clearcapital.oss.cassandra.configuration.WithMultiRingConfiguration;

public interface RecordTransformer {

	/**
	 * Attempt to transform records in given range.
	 * 
	 * @param startToken
	 * @param endToken
	 * @return number of records processed.
	 */
	public Long transformRecords(Long startToken, Long endToken) throws Exception;

	public void setConfiguration(WithMultiRingConfiguration configuration) throws Exception;

}
