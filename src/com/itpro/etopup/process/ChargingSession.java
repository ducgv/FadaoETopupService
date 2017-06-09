/**
 * 
 */
package com.itpro.etopup.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;

import com.itpro.etopup.main.Config;
import com.itpro.etopup.struct.ChargingCmd;
import com.itpro.log4j.ITProLog4jCategory;
import com.itpro.util.ProcessingThread;
import com.itpro.util.Queue;

/**
 * @author ducgv
 *
 */
public class ChargingSession extends ProcessingThread {
	private ChargingCmd chargingCmd;
	public ChargingSession(ChargingCmd chargingCmd, Queue queueResp, ITProLog4jCategory logger) {
		// TODO Auto-generated constructor stub
		this.chargingCmd = chargingCmd;
		this.queueResp = queueResp;
		this.logger = logger;
	}
	
	private Queue queueResp = null;
	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#OnHeartBeat()
	 */
	@Override
	protected void OnHeartBeat() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#initialize()
	 */
	@Override
	protected void initialize() {
		// TODO Auto-generated method stub
		setLogPrefix("[ChargingSession] ");
	}

	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#process()
	 */
	@Override
	protected void process() {
		// TODO Auto-generated method stub
		logInfo("Req "+chargingCmd);
		
		String cmd = "/opt/itpro/eTopup/scripts/chargeFee.php "
		        +Config.charging_spID+" "+ Config.charging_spPassword+" "+ Config.charging_serviceID+" "
		        + chargingCmd.recharge_msidn + " "+ chargingCmd.chargeValue+" "+chargingCmd.transactionID;

		Runtime run = Runtime.getRuntime();
		Process pr = null;
		try {
			pr = run.exec(cmd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			chargingCmd.charge_date = new Timestamp(System.currentTimeMillis());
			chargingCmd.resultCode = -3;
			chargingCmd.resultString = e.getMessage();
			queueResp.enqueue(chargingCmd);
			logError("Resp "+chargingCmd.toString());
			stop();
			return;
		}
		try {
			pr.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			chargingCmd.charge_date = new Timestamp(System.currentTimeMillis());
			chargingCmd.resultCode = -3;
			chargingCmd.resultString = e.getMessage();
			queueResp.enqueue(chargingCmd);
			logError("Resp "+chargingCmd.toString());
			stop();
			return;
		}
		BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
		String result = "";
		String line;
		try {
			while ((line = buf.readLine()) != null) {
				result += line;
			}
			buf.close();
			//logInfo("getSubInfo(" + msisdn + "):" + result);
			String[] arrResult = result.split("[|]");
			if(arrResult.length>=2){
				chargingCmd.charge_date = new Timestamp(System.currentTimeMillis());
				chargingCmd.resultCode = Integer.parseInt(arrResult[0]);
				chargingCmd.resultString = arrResult[1];
				queueResp.enqueue(chargingCmd);
				logInfo("Resp "+chargingCmd.toString());
				stop();
				return;
			}
			else{
				chargingCmd.charge_date = new Timestamp(System.currentTimeMillis());
				chargingCmd.resultCode = -3;
				chargingCmd.resultString = "Invalid ChargeFee result format";
				queueResp.enqueue(chargingCmd);
				logInfo("Resp "+chargingCmd.toString());
				stop();
				return;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			chargingCmd.charge_date = new Timestamp(System.currentTimeMillis());
			chargingCmd.resultCode = -3;
			chargingCmd.resultString = e.getMessage();
			queueResp.enqueue(chargingCmd);
			logInfo("Resp "+chargingCmd.toString());
			stop();
			return;
		}
	}
}
