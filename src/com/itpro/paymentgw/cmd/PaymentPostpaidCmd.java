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
public class PaymentPostpaidCmd extends PaymentGWCmd {
	public String msisdn;
	public String rechargeMsisdn;
	public int transactionId;
	public int amount;
	public Date reqDate;
	public String token;
	
	//response
	public int advanceBalance;
	public int debitBalance;
	
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "PaymentPostpaidReq: msisdn:"+rechargeMsisdn+
				"; transactionId:"+transactionId+
				"; reqDate:"+(new SimpleDateFormat("yyyyMMdd")).format(reqDate)+
				"; token:"+token;
				
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "PaymentPostpaidResp: msisdn:"+rechargeMsisdn+
				"; amount:"+amount+
				"; advanceBalance:"+advanceBalance+
				"; debitBalance:"+debitBalance+
				"; resultCode:"+resultCode+
				"; resultString:"+resultString;
	}

}
