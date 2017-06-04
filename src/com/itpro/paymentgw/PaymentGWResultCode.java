/**
 * 
 */
package com.itpro.paymentgw;

import java.util.Hashtable;

/**
 * @author Giap Van Duc
 *
 */
public class PaymentGWResultCode {
	public static final int R_SUCCESS = 0;
	public static final int R_ERROR = 1;
	//permanent
	
	public static final int RC_LOGIN_SUCCESS 			= 45000000; //Operation Successfully
	public static final int RC_KEEP_ALIVE_SUCCESS		= 405000000;
	public static final int RC_GET_SUBS_INFO_SUCCESS	= 405000000;
	public static final int RC_TOPUP_PREPAID_SUCCESS	= 405000000;
	public static final int RC_PAYMENT_POSTPAID_SUCCESS	= 405000000;
	
	//ext
	public static final int RC_CALL_SOAP_ERROR 	= 10000; //Call SOAP function error
	public static final int RC_TIMEOUT 			= 10001; //Timeout
	
	public static Hashtable<Integer, String> resultDesc = new Hashtable<Integer, String>();
	
	public static void init(){
		resultDesc.put(RC_CALL_SOAP_ERROR,"Call SOAP function error");
		resultDesc.put(RC_TIMEOUT,"Timeout");
	}

}
