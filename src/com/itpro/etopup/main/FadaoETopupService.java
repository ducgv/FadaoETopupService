/**
 * 
 */
package com.itpro.etopup.main;


import com.itpro.cfgreader.CfgReader;
import com.itpro.log4j.ITProLog4jCategory;
import com.itpro.util.MainForm;
import com.itpro.util.Params;

/**
 * @author Giap Van Duc
 *
 */
public class FadaoETopupService extends MainForm {
	private String configFile = "eTopupService.cfg";
	public static ITProLog4jCategory logger;
	/* (non-Javadoc)
	 * @see com.itpro.util.MainForm#OnLoadConfig()
	 */
	@Override
	protected void OnLoadConfig() {
		// TODO Auto-generated method stub
		CfgReader cfgReader = new CfgReader();
		String file = FadaoETopupService.getConfigPath()+configFile;
		cfgReader.load(file,";");		

		cfgReader.setGroup("DB");
		Config.dbServerName = cfgReader.getString("ServerIpAddr", "ServerIpAddr");
		Config.dbDatabaseName = cfgReader.getString("DbName", "DbName");
		Config.dbUserName = cfgReader.getString("UserName", "UserName");;
		Config.dbPassword = cfgReader.getString("Password", "Password");
		
		cfgReader.setGroup("PaymentGW");
		Config.spID = cfgReader.getString("SpID", "SpID");
		Config.spPassword = cfgReader.getString("Password", "Password");
		
		Config.charging_spID=cfgReader.getString("charging_spID", "charging_spID");
        Config.charging_spPassword=cfgReader.getString("charging_spPassword", "charging_spPassword");
        Config.charging_serviceID=cfgReader.getString("charging_serviceID", "charging_serviceID");
		
		cfgReader.setGroup("CLI");
		Config.cliListenPort = cfgReader.getInt("ListenPort", 1311);		
		Config.cliRequestTimeout = cfgReader.getInt("RequestTimeout", 60);
		
		if(cfgReader.isChanged())
			cfgReader.save(file);
		
		Config.smsMessageContents[Config.LANG_EN] = new Params();
		Config.smsMessageContents[Config.LANG_LA] = new Params();
		
		Config.ussdMessageContents[Config.LANG_EN] = new Params();
		Config.ussdMessageContents[Config.LANG_LA] = new Params();
	}

	/* (non-Javadoc)
	 * @see com.itpro.util.MainForm#OnStartSystem()
	 */
	@Override
	protected void OnStartSystem() {
		// TODO Auto-generated method stub
		logger = logManager.GetInstance("ETopup", getLogPath(), "ETopup", 1, 1, 1, 1, 1, 1, 1, true);
		
		GlobalVars.eTopupCli.setLogger(logger);
		GlobalVars.eTopupCli.setLogPrefix("[CLI] ");
		GlobalVars.eTopupCli.setRequestTimeout(Config.cliRequestTimeout);
		GlobalVars.eTopupCli.setListenPort(Config.cliListenPort);
		GlobalVars.eTopupCli.setTimeoutErrorString("Timeout");
		GlobalVars.eTopupCli.setSyntaxErrorString("Wrong Syntax");
		GlobalVars.eTopupCli.start();
		
		GlobalVars.serviceProcess.setLogger(logger);
		GlobalVars.serviceProcess.setLogPrefix("[ServiceProcess] ");
		GlobalVars.serviceProcess.start();
		
		GlobalVars.paymentGWInterface.setLogger(logger);
		GlobalVars.paymentGWInterface.setLogPrefix("[PaymentGWInterface] ");
		GlobalVars.paymentGWInterface.start();
		
		GlobalVars.insertSmsMTReqProcess.setLogPrefix("[SmsMT] ");
		GlobalVars.insertSmsMTReqProcess.setLogger(logger);
		GlobalVars.insertSmsMTReqProcess.start();
		
		GlobalVars.insertUssdNotifyReqProcess.setLogPrefix("[USSDNotify] ");
		GlobalVars.insertUssdNotifyReqProcess.setLogger(logger);
		GlobalVars.insertUssdNotifyReqProcess.start();
		
		GlobalVars.insertTransactionRecordProcess.setLogPrefix("[Transaction] ");
		GlobalVars.insertTransactionRecordProcess.setLogger(logger);
		GlobalVars.insertTransactionRecordProcess.start();
		
        GlobalVars.updateBatchRechargeElementProcess.setLogPrefix("[UpdateBatchRechargeElement] ");
        GlobalVars.updateBatchRechargeElementProcess.setLogger(logger);
        GlobalVars.updateBatchRechargeElementProcess.start();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FadaoETopupService.setHomePath("/opt/itpro/eTopup/eTopupService/");
		FadaoETopupService.setLogConfig("loggerETopupService.conf");
		FadaoETopupService main = new FadaoETopupService();				
		main.start();
	}

}
