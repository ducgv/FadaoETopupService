/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import com.itpro.etopup.struct.DealerRequest;

/**
 * @author ducgv
 *
 */
public class QueryBalanceCmd extends RequestCmd {
	public long dealerAccountBalance;

	public QueryBalanceCmd() {
		// TODO Auto-generated constructor stub
		cmdType = DealerRequest.CMD_TYPE_QUERY_BALANCE;
	}
	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.RequestCmd#getReqString()
	 */
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		String str = "QueryBalanceReq: msisdn:"+msisdn;
		return str;
	}

	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.RequestCmd#getRespString()
	 */
	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		String str = "QueryBalanceResp: msisdn:"+msisdn;
		if(resultCode==R_OK)
			str+="; dealerAccountBalance:"+dealerAccountBalance;
		else{
			str+="; resultCode:"+resultCode;
			str+="; resultString:"+resultString;
		}
		return str;
	}

}
