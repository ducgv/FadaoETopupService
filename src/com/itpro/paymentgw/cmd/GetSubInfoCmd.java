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
public class GetSubInfoCmd extends PaymentGWCmd {
	public static final int SUBS_TYPE_PREPAID = 0;
	public static final int SUBS_TYPE_POSTPAID = 1;
	
	public String msisdn;
	public String rechargeMsisdn;
	public int transactionId;
	public Date reqDate;
	public String token;
	
	//response
	public Date activeDate;
	public int subType;
	public int state;
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "GetSubInfoReq: msisdn:"+rechargeMsisdn+
				"; transactionId:"+transactionId+
				"; reqDate:"+(new SimpleDateFormat("yyyyMMdd")).format(reqDate)+
				"; token:"+token;
				
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "GetSubInfoResp: msisdn:"+rechargeMsisdn+
				"; transactionId:"+transactionId+
				"; activeDate:"+(new SimpleDateFormat("yyyy-MM-dd")).format(activeDate)+
				"; subType:"+subType+
				"; state:"+state;
	}

}
