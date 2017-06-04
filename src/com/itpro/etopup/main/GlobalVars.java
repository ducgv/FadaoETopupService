/**
 * 
 */
package com.itpro.etopup.main;

import com.itpro.etopup.cli.ETopupCli;
import com.itpro.etopup.process.ServiceProcess;
import com.itpro.etopup.process.InsertSmsMTReqProcess;
import com.itpro.etopup.process.InsertTransactionRecordProcess;
import com.itpro.etopup.process.InsertUssdNotifyReqProcess;
import com.itpro.paymentgw.PaymentGWInterface;

/**
 * @author Giap Van Duc
 *
 */
public class GlobalVars {
	public static ServiceProcess serviceProcess = new ServiceProcess();
	public static PaymentGWInterface paymentGWInterface = new PaymentGWInterface();
	public static ETopupCli eTopupCli = new ETopupCli();
	public static boolean stopModuleFlag = false;
	public static InsertSmsMTReqProcess insertSmsMTReqProcess = new InsertSmsMTReqProcess();
	public static InsertUssdNotifyReqProcess insertUssdNotifyReqProcess = new InsertUssdNotifyReqProcess();
	public static InsertTransactionRecordProcess insertTransactionRecordProcess = new InsertTransactionRecordProcess();
}
