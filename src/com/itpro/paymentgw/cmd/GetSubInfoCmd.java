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
	public int transactionId;
	public Date reqDate;
	public String token;
	
	//response
	public Date activeDate;
	public int subType;
	public int state;
	public int balance = 0;
	public String subId = "";
	public String detail = "";
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "GetSubInfoReq: msisdn:"+msisdn+
				"; transactionId:"+transactionId+
				"; reqDate:"+(new SimpleDateFormat("yyyyMMdd")).format(reqDate)+
				"; token:"+token;
				
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "GetSubInfoResp: msisdn:"+msisdn+
				"; transactionId:"+transactionId+
				"; activeDate:"+activeDate==null?"NULL":(new SimpleDateFormat("yyyy-MM-dd")).format(activeDate)+
				"; subType:"+subType+
				"; subId:"+subId+
				"; balance:"+balance+
				"; state:"+state+
				"; resultCode:"+resultCode+
				"; resultString:"+resultString;
				
	}

}
