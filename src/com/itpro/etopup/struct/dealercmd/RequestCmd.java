/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import com.itpro.etopup.struct.DealerInfo;

/**
 * @author ducgv
 *
 */
public abstract class RequestCmd {
	public final static int R_OK = 0;
	public final static int R_SYSTEM_ERROR = 1;
	public final static int R_TELCO_ERROR = 2;
	public final static int R_CUSTOMER_INFO_FAIL = 3;
	public int cmdType;
	public String msisdn;
	public DealerInfo dealerInfo;
	public String pinCode;
	public int resultCode;
	public String resultString;
	public abstract String getReqString();
	public abstract String getRespString();
}
