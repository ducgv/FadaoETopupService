/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import com.itpro.etopup.struct.DealerRequest;

/**
 * @author ducgv
 *
 */
public class RechargeCmd extends RequestCmd {
	public String rechargeMsisdn;
	public int amount;
	public boolean checkPin = true;
	public int db_return_code;
	public int balanceAfter;
	public RechargeCmd() {
		// TODO Auto-generated constructor stub
		cmdType = DealerRequest.CMD_TYPE_RECHARGE;
	}
	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.RequestCmd#getReqString()
	 */
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "RechargeReq: msisdn:"+rechargeMsisdn+
				"; amount:"+amount;
	}

	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.RequestCmd#getRespString()
	 */
	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "RechargeResp: msisdn:"+rechargeMsisdn+
				"; resultCode:"+resultCode+
				"; resultString:"+resultString;
	}

}
