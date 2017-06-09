/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import java.util.Vector;

import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;

/**
 * @author ducgv
 *
 */
public class BatchRechargeCmd extends RequestCmd {
	public static final int CHECK_STATUS_OK = 0;
	public static final int CHECK_STATUS_DEALER_INFO_NOT_MATCHED = 1;
	public static final int CHECK_STATUS_BATCH_RECHARGE_LIST_NOT_FOUND = 2;
	public static final int CHECK_STATUS_INVALID_RECEIVER = 3;
	public static final int CHECK_STATUS_INVALID_AMOUNT = 4;
	public static final int CHECK_STATUS_DOES_NOT_ENOUGH_BALANCE = 4;
	
	public int batch_recharge_id = 0;
	public Vector<BatchRechargeElement> batchRechargeElements;
	public int batch_recharge_total_amount = 0;
	public int total_element = 0;
	public int recharge_success = 0;
	public int recharge_success_amount = 0;
	public int recharge_failed = 0;
	public long lastBalanceAfter = 0;
	public BatchRechargeElement currentBatchRechargeElement = null;
	public int checkInfo(DealerInfo dealerInfo){
		boolean isDealerInfoNotMatched = false;
		boolean isInvalidReceiver = false;
		boolean isInvalidAmount = false;
		batch_recharge_total_amount = 0;
		if(!batchRechargeElements.isEmpty()){
			for(BatchRechargeElement batchRechargeElement:batchRechargeElements){
				batch_recharge_total_amount+=batchRechargeElement.recharge_value;
				if(!batchRechargeElement.dealer_msisdn.equals(dealerInfo.msisdn)||batchRechargeElement.dealer_id!=dealerInfo.id){
					isDealerInfoNotMatched = true;
				}
				if(!((batchRechargeElement.recharge_msisdn.startsWith("202") && batchRechargeElement.recharge_msisdn.length()==10)
						||(batchRechargeElement.recharge_msisdn.startsWith("302") && batchRechargeElement.recharge_msisdn.length()==9))){
					isInvalidReceiver = true;
				}
				if(batchRechargeElement.recharge_value<1000||batchRechargeElement.recharge_value%1000>0){
					isInvalidAmount = true;
				}
			}
			total_element = batchRechargeElements.size();
		}
		else{
			total_element = 0;
			batch_recharge_total_amount = 0;
		}
		recharge_failed = 0;
		if(isDealerInfoNotMatched)
			return CHECK_STATUS_DEALER_INFO_NOT_MATCHED;
		if(total_element ==0)
			return CHECK_STATUS_BATCH_RECHARGE_LIST_NOT_FOUND;
		if(isInvalidReceiver)
			return CHECK_STATUS_INVALID_RECEIVER;
		if(isInvalidAmount)
			return CHECK_STATUS_INVALID_AMOUNT;
		if(batch_recharge_total_amount>dealerInfo.balance)
			return CHECK_STATUS_DOES_NOT_ENOUGH_BALANCE;
		return CHECK_STATUS_OK;
	}
	public BatchRechargeCmd() {
		// TODO Auto-generated constructor stub
		cmdType = DealerRequest.CMD_TYPE_BATCH_RECHARGE;
	}
	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.dealercmd.RequestCmd#getReqString()
	 */
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "BatchRechargeReq: msisdn:"+msisdn+
				"; batch_recharge_id:"+batch_recharge_id;
	}

	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.dealercmd.RequestCmd#getRespString()
	 */
	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "BatchRechargeResp: msisdn:"+msisdn+
				"; batch_recharge_id:"+batch_recharge_id+
				"; total_element:"+total_element+
				"; recharge_success:"+recharge_success+
				"; recharge_failed:"+recharge_failed+
				"; batch_recharge_total_amount:"+batch_recharge_total_amount+
				"; recharge_success_amount:"+recharge_success_amount+
				"; resultCode:"+resultCode+
				"; resultString:"+resultString;
	}
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return String.format("TotalSubs: %d, TotalAmount:%d, Success:%d, Failed:%d, RechargeSuccessAmount:%d", total_element, batch_recharge_total_amount, recharge_success, recharge_failed, recharge_success_amount);
	}
	
	

}
