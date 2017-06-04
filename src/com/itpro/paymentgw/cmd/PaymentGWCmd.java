/**
 * 
 */
package com.itpro.paymentgw.cmd;

import com.itpro.util.Queue;

/**
 * @author ducgv
 *
 */
public abstract class PaymentGWCmd {
	public Queue queueResp;
	public int seq;
	public int result;	//0: success, 1: failed;
	public int resultCode;
	public String resultString;
	public long reqTime;
	public abstract String getReqString();
	public abstract String getRespString();
}
