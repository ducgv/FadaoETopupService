/**
 * 
 */
package com.itpro.paymentgw.cmd;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ducgv
 *
 */
public class TopupPrepaidCmd extends PaymentGWCmd {
	public String msisdn;
	public String rechargeMsisdn;
	public int transactionId;
	public int amount;
	public Date reqDate;
	public String token;
	
	//response
	public int currentBalance;
	public Date newActiveDate;
	
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "TopupPrepaidReq: msisdn:"+rechargeMsisdn+
				"; transactionId:"+transactionId+
				"; reqDate:"+(new SimpleDateFormat("yyyyMMdd")).format(reqDate)+
				"; token:"+token;
				
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "TopupPrepaidResp: msisdn:"+rechargeMsisdn+
				"; amount:"+amount+
				"; currentBalance:"+currentBalance+
				"; newActiveDate:"+(new SimpleDateFormat("yyyy-MM-dd")).format(newActiveDate)+
				"; resultCode:"+resultCode+
				"; resultString:"+resultString;
	}

}
