/**
 * 
 */
package com.itpro.paymentgw.cmd;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.itpro.etopup.struct.Promotion;

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
	public GetSubInfoCmd subInfo;
	public int balanceBonus = 0;
	public int dataBonus = 0;
	public String originalNumber;
	
	public Promotion promotionBalanceInfo;
	public Promotion promotionDataInfo;
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "PaymentPostpaidReq: msisdn:"+rechargeMsisdn+
				"; transactionId:"+transactionId+
				"; reqDate:"+(new SimpleDateFormat("yyyyMMdd")).format(reqDate)+
				"; balanceBonus:"+balanceBonus+
				"; dataBonus:"+dataBonus+
				"; originalNumber:"+originalNumber+
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
