/**
 * 
 */
package com.itpro.etopup.main;

import com.itpro.util.Params;

/**
 * @author Giap Van Duc
 *
 */
public class Config {
	//CLI
	public static int		cliListenPort;	
	public static int 		cliRequestTimeout;
	
	//DB
	public static String 	dbServerName;
	public static String 	dbDatabaseName;
	public static String 	dbUserName;
	public static String 	dbPassword;
	
	//PaymentGW
	public static String 	spID;
	public static String 	spPassword;
	
	public static final int LANG_EN = 0;
	public static final int LANG_LA = 1;
	public static int smsLanguage;
	
//	public static Object mutex = new Object();
	public static Params[]	smsMessageContents = new Params[2];
	public static Params[]	ussdMessageContents = new Params[2];
	public static Params serviceConfigs = new Params();
	
    // charging
    public static String    charging_spID;
    public static String    charging_serviceID;
    public static String    charging_spPassword;
    public static int       MULTIPLIER=100;
	public static int consecutiveTransactionDelayTime;
}
