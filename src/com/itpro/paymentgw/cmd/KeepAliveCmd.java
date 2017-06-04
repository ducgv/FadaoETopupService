/**
 * 
 */
package com.itpro.paymentgw.cmd;

/**
 * @author ducgv
 *
 */
public class KeepAliveCmd extends PaymentGWCmd {
	public String token;
	
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "KeepAlive: token:"+token;
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "KeepAliveResp: token:"+token+"; result:"+result+"; resultCode:"+resultCode+"; resultString:"+resultString;
	}

}
