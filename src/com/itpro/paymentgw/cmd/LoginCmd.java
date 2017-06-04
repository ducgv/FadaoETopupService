/**
 * 
 */
package com.itpro.paymentgw.cmd;

/**
 * @author ducgv
 *
 */
public class LoginCmd extends PaymentGWCmd {
	public String spID;
	public String spPassword;
	public int transactionId;
	public String token;
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "Login: spID:"+spID+"; transactionId:"+transactionId;
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "LoginResp: spID:"+spID+"; transactionId:"+transactionId+"; result:"+result+"; resultCode:"+resultCode+"; resultString:"+resultString+"; token:"+token;
	}

}
