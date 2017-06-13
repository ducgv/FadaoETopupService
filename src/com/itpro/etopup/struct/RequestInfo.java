/**
 * 
 */
package com.itpro.etopup.struct;

import com.itpro.etopup.struct.dealercmd.BatchRechargeElement;

/**
 * @author ducgv
 *
 */
public class RequestInfo {
	public String msisdn;
	public DealerRequest dealerRequest = null;
	public AgentRequest agentRequest = null;
	public DealerInfo dealerInfo = null;
	public TransactionRecord transactionRecord = null;
	public TransactionRecord old_transactionRecord = null;
	public BatchRechargeElement batchRechargeElement=null;
}
