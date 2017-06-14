/**
 * 
 */
package com.itpro.etopup.process;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Random;
import java.util.Vector;
import com.itpro.etopup.db.DbConnection;
import com.itpro.etopup.main.Config;
import com.itpro.etopup.main.GlobalVars;
import com.itpro.etopup.struct.AgentInfo;
import com.itpro.etopup.struct.AgentRequest;
import com.itpro.etopup.struct.ChargingCmd;
import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;
import com.itpro.etopup.struct.DelayMoveStock;
import com.itpro.etopup.struct.DelayRecharge;
import com.itpro.etopup.struct.MTRecord;
import com.itpro.etopup.struct.MoveDealerProvinceCmd;
import com.itpro.etopup.struct.RechargeCdrRecord;
import com.itpro.etopup.struct.RequestInfo;
import com.itpro.etopup.struct.SmsTypes;
import com.itpro.etopup.struct.TransactionRecord;
import com.itpro.etopup.struct.dealercmd.BatchRechargeCmd;
import com.itpro.etopup.struct.dealercmd.BatchRechargeElement;
import com.itpro.etopup.struct.dealercmd.ChangePinCmd;
import com.itpro.etopup.struct.dealercmd.MoveStockCmd;
import com.itpro.etopup.struct.dealercmd.QueryBalanceCmd;
import com.itpro.etopup.struct.dealercmd.RechargeCmd;
import com.itpro.etopup.struct.dealercmd.RequestCmd;
import com.itpro.paymentgw.PaymentGWResultCode;
import com.itpro.paymentgw.cmd.GetSubInfoCmd;
import com.itpro.paymentgw.cmd.KeepAliveCmd;
import com.itpro.paymentgw.cmd.LoginCmd;
import com.itpro.paymentgw.cmd.PaymentGWCmd;
import com.itpro.paymentgw.cmd.PaymentPostpaidCmd;
import com.itpro.paymentgw.cmd.TopupPrepaidCmd;
import com.itpro.util.MySQLConnection;
import com.itpro.util.ProcessingThread;
import com.itpro.util.Queue;
import com.itpro.util.StringFunction;

/**
 * @author Giap Van Duc
 *
 */
public class ServiceProcess extends ProcessingThread {
	public final long millisecondsOfHour = 3600000;
	DbConnection connection = null;
	public boolean isConnected = false;
	private long nextTimeGetDealerRequests = System.currentTimeMillis();
	private long nextTimeGetAgentRequests = System.currentTimeMillis();
	//private long curTime = System.currentTimeMillis();

	public static final int LOGIN_STATE_NONE = 0;
	public static final int LOGIN_STATE_WAIT = 1;
	public static final int LOGIN_STATE_SUCCESS = 2; 

	public String token = "";
	public int loginState = 0; //0: not login, 1: loging in, 2: logged in;
	public Queue queuePaymentGWResp = new Queue();
	public Queue queueChargingCmdResp = new Queue();
	public long lastKeepAliveTime = 0;
	public long lastCheckDelayFlagTime = 0;
	
	private Vector<DealerRequest> dealerRequests = new Vector<DealerRequest>();
	private Vector<AgentRequest> agentRequests = new Vector<AgentRequest>();
	private Hashtable<String, RequestInfo> listRequestProcessing = new Hashtable<String, RequestInfo>();
	
	private Hashtable<String, DelayRecharge> listDelayRecharges = new Hashtable<String, DelayRecharge>();
	private Hashtable<String, DelayMoveStock> listDelayMoveStocks = new Hashtable<String, DelayMoveStock>();
	
	public int getPaymentGWTransactionId(){
		int transactionId;
		try {
			transactionId = connection.getPaymentGWTransactionId();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logError("getPaymentGWTransactionId error:"+MySQLConnection.getSQLExceptionString(e));
			transactionId = -1;
		}
		return transactionId;
	}
	
	private void login() {
		// TODO Auto-generated method stub
		LoginCmd loginCmd = new LoginCmd();
		loginCmd.spID = Config.spID;
		loginCmd.spPassword = Config.spPassword;
		loginCmd.transactionId = getPaymentGWTransactionId();
		loginCmd.queueResp = queuePaymentGWResp;
		if(loginCmd.transactionId!=-1){
			logInfo(loginCmd.getReqString());
			GlobalVars.paymentGWInterface.queueUserRequest.enqueue(loginCmd);
			loginState = LOGIN_STATE_WAIT;
		}
	}
	
	private void OnLoginResp(LoginCmd loginCmdResp) {
		// TODO Auto-generated method stub
		logInfo(loginCmdResp.getRespString());
		if(loginCmdResp.result == PaymentGWResultCode.R_SUCCESS&&loginCmdResp.resultCode == PaymentGWResultCode.RC_LOGIN_SUCCESS){
			token = loginCmdResp.token;
			loginState = LOGIN_STATE_SUCCESS;
			lastKeepAliveTime = System.currentTimeMillis();
		}
		else{
			loginState = LOGIN_STATE_NONE;
		}
	}

	private void keepAlive() {
		// TODO Auto-generated method stub
		KeepAliveCmd keepAliveCmd = new KeepAliveCmd();
		keepAliveCmd.token = token;
		keepAliveCmd.queueResp = queuePaymentGWResp;
		logInfo(keepAliveCmd.getReqString());
		GlobalVars.paymentGWInterface.queueUserRequest.enqueue(keepAliveCmd);

	}
	
	private void OnKeepAliveResp(KeepAliveCmd keepAliveCmdResp) {
		// TODO Auto-generated method stub
		logInfo(keepAliveCmdResp.getRespString());
		if(keepAliveCmdResp.result == PaymentGWResultCode.R_SUCCESS&&keepAliveCmdResp.resultCode == PaymentGWResultCode.RC_KEEP_ALIVE_SUCCESS){
			lastKeepAliveTime = System.currentTimeMillis();
		}
		else{
			loginState = LOGIN_STATE_NONE;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.itpro.util.ProcessingThread#OnHeartBeat()
	 */
	@Override
	protected void OnHeartBeat() {
		// TODO Auto-generated method stub
		if (connection == null) {
			Connect();
		} else if (!isConnected) {
			connection.close();
			Connect();
		}
		if(isConnected){
			if(loginState == LOGIN_STATE_NONE){
				login();
			}
			else if(loginState==LOGIN_STATE_SUCCESS){
				long curTime = System.currentTimeMillis();
				if(curTime>=lastKeepAliveTime+150000){
					keepAlive();
					lastKeepAliveTime=curTime;
				}
			}
			try {
				connection.checkConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				logError("Check DB connection error:"+e.getMessage());
				isConnected = false;
			}
			
		}
		
		long curTime = System.currentTimeMillis();
		if(curTime>=lastCheckDelayFlagTime+300000){
			checkDelayTransactionFlagsTimeout();
			lastCheckDelayFlagTime=curTime;
		}
	}
	
	
	private void checkDelayTransactionFlagsTimeout() {
		// TODO Auto-generated method stub
		Enumeration<String> keys = listDelayRecharges.keys();
		long currentTime = System.currentTimeMillis();
		while (keys.hasMoreElements()){
			String key = keys.nextElement();
			DelayRecharge delayRecharge = listDelayRecharges.get(key);
			if(currentTime >= delayRecharge.timestamp+Config.consecutiveTransactionDelayTime*60000){
				listDelayRecharges.remove(key);
			}
		}
		
		keys = listDelayMoveStocks.keys();
		while (keys.hasMoreElements()){
			String key = keys.nextElement();
			DelayMoveStock delayMoveStock = listDelayMoveStocks.get(key);
			if(currentTime >= delayMoveStock.timestamp+Config.consecutiveTransactionDelayTime*60000){
				listDelayMoveStocks.remove(key);
			}
		}
	}

	private void OnConnected() {
		logInfo("Connected to DB");
		try {
			if(!Config.smsMessageContents[Config.LANG_EN].isLoaded)
				connection.getParams(Config.smsMessageContents[Config.LANG_EN], "msg_content_sms_en");
			if(!Config.smsMessageContents[Config.LANG_LA].isLoaded)
				connection.getParams(Config.smsMessageContents[Config.LANG_LA], "msg_content_sms_la");
			
			if(!Config.ussdMessageContents[Config.LANG_EN].isLoaded)
				connection.getParams(Config.ussdMessageContents[Config.LANG_EN], "msg_content_ussd_en");
			if(!Config.ussdMessageContents[Config.LANG_LA].isLoaded)
				connection.getParams(Config.ussdMessageContents[Config.LANG_LA], "msg_content_ussd_la");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logError("Load MessageContents error:" + MySQLConnection.getSQLExceptionString(e));
			isConnected = false;
		}
		try {
			if(!Config.serviceConfigs.isLoaded)	
				connection.getParams(Config.serviceConfigs, "service_config");

			Config.smsLanguage = Config.LANG_EN;
			String smsLanguage = Config.serviceConfigs.getParam("SMS_LANGUAGE");
			if (smsLanguage.equalsIgnoreCase("LA")){
				Config.smsLanguage = Config.LANG_LA;
			}
			
			Config.consecutiveTransactionDelayTime = Integer.parseInt(Config.serviceConfigs.getParam("CONSECUTIVE_TRANSACTION_DELAY_TIME"));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logError("Load ServiceConfigs error:" + MySQLConnection.getSQLExceptionString(e));
			isConnected = false;
		}
	}

	private void Connect() {
		connection = new DbConnection(Config.dbServerName, Config.dbDatabaseName, Config.dbUserName, Config.dbPassword);
		Exception exception = null;
		try {
			isConnected = connection.connect();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			exception = e;
		}

		if (exception != null) {
			isConnected = false;
			logError("Connect to DB error:" + exception.getMessage());
		} else {
			OnConnected();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.itpro.util.ProcessingThread#initialize()
	 */
	@Override
	protected void initialize() {
		// TODO Auto-generated method stub
		lastCheckDelayFlagTime = System.currentTimeMillis();
		setHeartBeatInterval(5000);
		Connect();
//		nextTime = System.currentTimeMillis();
		if(isConnected){
			login();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.itpro.util.ProcessingThread#process()
	 */
	@Override
	protected void process() {
		// TODO Auto-generated method stub
		if (connection == null || !isConnected)
			return;
		
		PaymentGWCmd paymentGWCmdResp = (PaymentGWCmd)queuePaymentGWResp.dequeue();
		if(paymentGWCmdResp!=null){
			if (paymentGWCmdResp instanceof LoginCmd) {
				LoginCmd loginCmdResp = (LoginCmd) paymentGWCmdResp;
				OnLoginResp(loginCmdResp);
			}
			else if (paymentGWCmdResp instanceof KeepAliveCmd) {
				KeepAliveCmd keepAliveCmdResp = (KeepAliveCmd) paymentGWCmdResp;
				OnKeepAliveResp(keepAliveCmdResp);
			}
			else if (paymentGWCmdResp instanceof GetSubInfoCmd) {
				GetSubInfoCmd getSubInfoCmdResp = (GetSubInfoCmd) paymentGWCmdResp;
				OnGetSubInfoResp(getSubInfoCmdResp);
			}
			else if (paymentGWCmdResp instanceof TopupPrepaidCmd) {
				TopupPrepaidCmd topupPrepaidCmdResp = (TopupPrepaidCmd) paymentGWCmdResp;
				OnTopupPrepaidCmdResp(topupPrepaidCmdResp);
			}
			else if (paymentGWCmdResp instanceof PaymentPostpaidCmd) {
				PaymentPostpaidCmd paymentPostpaidCmdResp = (PaymentPostpaidCmd) paymentGWCmdResp;
				OnPaymentPostpaidCmdResp(paymentPostpaidCmdResp);
			}

		}
        ChargingCmd chargingCmdResp = (ChargingCmd) queueChargingCmdResp.dequeue();
        if(chargingCmdResp!=null){
            OnChargingCmdResp(chargingCmdResp);
        }
		getDealerRequests();
		if (!dealerRequests.isEmpty()) {
			DealerRequest dealerRequest = dealerRequests.remove(0);
			OnDealerRequest(dealerRequest);
		}
		
		getAgentRequests();
		if (!agentRequests.isEmpty()) {
			AgentRequest agentRequest = agentRequests.remove(0);
			OnAgentRequest(agentRequest);
		}
	}
	
	private void OnAgentRequest(AgentRequest agentRequest) {
		// TODO Auto-generated method stub
		logInfo(agentRequest.getReqString());
		if(listRequestProcessing.containsKey(agentRequest.dealer_msisdn)){
			agentRequests.add(agentRequest);
			return;
		}
		
		RequestInfo requestInfo = new RequestInfo();
		requestInfo.msisdn = agentRequest.dealer_msisdn;
		requestInfo.agentRequest = agentRequest;
		listRequestProcessing.put(requestInfo.msisdn, requestInfo);
		
		switch(agentRequest.req_type){
		case AgentRequest.REQ_TYPE_CREATE_DEALER:
			OnCreateDealer(requestInfo);
			break;
		case AgentRequest.REQ_TYPE_ADD_BALANCE:
			OnAddBalance(requestInfo);
			break;
		case AgentRequest.REQ_TYPE_REFUND:
		case AgentRequest.REQ_TYPE_CANCEL_ADD_BALANCE:
			onRefund(requestInfo);
		    break;
		case AgentRequest.REQ_TYPE_MOVE_DEALER_PROVINCE:
			OnMoveDealerProvince(requestInfo);
		default:
			break;
		}
	}

	private void OnMoveDealerProvince(RequestInfo requestInfo) {
		// TODO Auto-generated method stub
		DealerInfo oldDealerInfo = null;
		AgentRequest agentRequest = requestInfo.agentRequest;
		try {
			oldDealerInfo = connection.getDealerInfo(requestInfo.msisdn);
			requestInfo.dealerInfo = oldDealerInfo;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(oldDealerInfo==null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DEALER_NOT_FOUND;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			logInfo("AddBalance: msisdn:"+requestInfo.msisdn +"; error: Number not is Dealer");
		}
		
		AgentInfo agentInitInfo = null;
		try {
			agentInitInfo = connection.getAgentInfo(agentRequest.agent_id);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(agentInitInfo == null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_AGENT_INIT_NOT_FOUND;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		AgentInfo agentApprovedInfo = null;
		try {
			agentApprovedInfo = connection.getAgentInfo(agentRequest.agent_approved_id);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(agentApprovedInfo == null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_AGENT_APPROVED_NOT_FOUND;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		
		MoveDealerProvinceCmd moveDealerProvinceCmd = new MoveDealerProvinceCmd();
		moveDealerProvinceCmd.dealer_id = oldDealerInfo.id;
		moveDealerProvinceCmd.new_provice_code = agentApprovedInfo.province_code;
		moveDealerProvinceCmd.approved = agentApprovedInfo.user_name;
		moveDealerProvinceCmd.approved_id = agentApprovedInfo.id;
		try {
			connection.moveDealerProvince(moveDealerProvinceCmd);
			if(moveDealerProvinceCmd.return_code==0){
				TransactionRecord transactionRecord = createTransactionRecord();
				transactionRecord.dealer_msisdn = requestInfo.msisdn;
				transactionRecord.dealer_id = oldDealerInfo.id;
				transactionRecord.dealer_province = oldDealerInfo.province_register;
				transactionRecord.transaction_amount_req = -1*oldDealerInfo.balance;
				transactionRecord.balance_changed_amount = -1*oldDealerInfo.balance;
				transactionRecord.balance_before = oldDealerInfo.balance;
				transactionRecord.balance_after = 0;
				transactionRecord.type = TransactionRecord.TRANS_TYPE_MOVE_DEALER_OLD_PROVINCE;
				transactionRecord.agent = agentInitInfo.user_name;
				transactionRecord.agent_id = agentInitInfo.id;
				transactionRecord.approved = agentApprovedInfo.user_name;
				transactionRecord.approved_id = agentApprovedInfo.id;
				transactionRecord.dealer_new_id = moveDealerProvinceCmd.new_dealer_id;
				transactionRecord.dealer_new_province = moveDealerProvinceCmd.new_provice_code;
				transactionRecord.result_description = "Move dealer province successfully";
				transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
				transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
				insertTransactionRecord(transactionRecord);
				
				
				
				agentRequest.status = AgentRequest.STATUS_SUCCESS;
				agentRequest.dealer_id = oldDealerInfo.id;
				agentRequest.result_code = AgentRequest.RC_MOVE_DEALER_PROVINCE_SUCCESS;
				agentRequest.transaction_id = transactionRecord.id;
				updateAgentRequest(agentRequest);
				logInfo(agentRequest.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_MOVE_DEALER_PROVINCE_SUCCESS")
						.replaceAll("<PIN>", oldDealerInfo.pin_code)
						.replaceAll("<BALANCE>", ""+oldDealerInfo.balance);
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_MOVE_DEALER_PROVINCE_SUCCESS")
						.replaceAll("<PIN>", oldDealerInfo.pin_code)
						.replaceAll("<BALANCE>", ""+oldDealerInfo.balance);

				sendSms(requestInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_DEALER_PROVINCE, transactionRecord.id);


				// send web user:
				String contentWebNotify = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REGISTER_DEALER_NOTIFY_WEB_USER")
						.replaceAll("<DEALER>", agentRequest.dealer_msisdn.replaceFirst("856", "0"))
						.replaceAll("<WEB_PASSWORD>", oldDealerInfo.web_password);
				MTRecord mtRecord = new MTRecord(agentRequest.dealer_msisdn, contentWebNotify, SmsTypes.SMS_TYPE_MOVE_DEALER_PROVINCE, transactionRecord.id);
				GlobalVars.insertSmsMTReqProcess.queueInsertMTReq.enqueue(mtRecord);
				logInfo("SendSms: msisdn:" + agentRequest.dealer_msisdn + "; content:" + contentWebNotify);
				
				transactionRecord = createTransactionRecord();
				transactionRecord.dealer_msisdn = requestInfo.msisdn;
				transactionRecord.dealer_id = moveDealerProvinceCmd.new_dealer_id;
				transactionRecord.dealer_province = moveDealerProvinceCmd.new_provice_code;
				transactionRecord.transaction_amount_req = oldDealerInfo.balance;
				transactionRecord.balance_changed_amount = oldDealerInfo.balance;
				transactionRecord.balance_before = 0;
				transactionRecord.balance_after = oldDealerInfo.balance;
				transactionRecord.type = TransactionRecord.TRANS_TYPE_MOVE_DEALER_NEW_PROVINCE_ACCEPTED;
				transactionRecord.agent = agentApprovedInfo.user_name;
				transactionRecord.agent_id = agentApprovedInfo.id;
				transactionRecord.approved = agentApprovedInfo.user_name;
				transactionRecord.approved_id = agentApprovedInfo.id;
				transactionRecord.result_description = "Accep dealer with new province successfully";
				transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
				transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
				insertTransactionRecord(transactionRecord);
				
				listRequestProcessing.remove(requestInfo.msisdn);
			}
			else{
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.dealer_id = oldDealerInfo.id;
				agentRequest.result_code = AgentRequest.RC_MOVE_DEALER_PROVINCE_FAILED;
				updateAgentRequest(agentRequest);
				logInfo(agentRequest.getRespString());
				listRequestProcessing.remove(requestInfo.msisdn);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.dealer_id = oldDealerInfo.id;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			updateAgentRequest(agentRequest);
			logInfo(agentRequest.getRespString());
			listRequestProcessing.remove(requestInfo.msisdn);
		}
		
	}

	private void OnAddBalance(RequestInfo requestInfo) {
		// TODO Auto-generated method stub
		DealerInfo dealerInfo = null;
		AgentRequest agentRequest = requestInfo.agentRequest;
		try {
			dealerInfo = connection.getDealerInfo(requestInfo.msisdn);
			requestInfo.dealerInfo = dealerInfo;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(dealerInfo==null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DEALER_NOT_FOUND;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			logInfo("AddBalance: msisdn:"+requestInfo.msisdn +"; error: Number not is Dealer");
		}
		else{
			AgentInfo agentInitInfo = null;
			try {
				agentInitInfo = connection.getAgentInfo(agentRequest.agent_id);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				isConnected = false;
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
				logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			if(agentInitInfo == null){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_AGENT_INIT_NOT_FOUND;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			AgentInfo agentApprovedInfo = null;
			try {
				agentApprovedInfo = connection.getAgentInfo(agentRequest.agent_approved_id);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				isConnected = false;
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
				logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			if(agentApprovedInfo == null){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_AGENT_APPROVED_NOT_FOUND;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			
			if(agentInitInfo.province_code!=dealerInfo.province_register){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_DEALER_IS_OUTSIDE_PROVINCE;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			else if(dealerInfo.parent_id>0){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_ADD_BALANCE_ACCOUNT_IS_SUB_DEALER;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			else {
					TransactionRecord transactionRecord = createTransactionRecord();
					agentRequest.balance_add_amount = agentRequest.addBalanceInfo.cash_value+agentRequest.addBalanceInfo.commision_value;
					agentRequest.dealer_id = dealerInfo.id;
					transactionRecord.balance_before = dealerInfo.balance;
					dealerInfo.balance += agentRequest.balance_add_amount;
					updateDealer(agentRequest);
					transactionRecord.dealer_msisdn = requestInfo.msisdn;
					transactionRecord.dealer_id = dealerInfo.id;
					transactionRecord.balance_after = dealerInfo.balance;
					transactionRecord.type = TransactionRecord.TRANS_TYPE_ADD_BALANCE;
					transactionRecord.transaction_amount_req = agentRequest.balance_add_amount;
					transactionRecord.balance_changed_amount = agentRequest.balance_add_amount;
					transactionRecord.agent = agentInitInfo.user_name;
					transactionRecord.agent_id = agentInitInfo.id;
					transactionRecord.approved = agentApprovedInfo.user_name;
					transactionRecord.agent_id = agentApprovedInfo.id;
					transactionRecord.addBalanceInfo = agentRequest.addBalanceInfo;
					transactionRecord.result_description = "Add ETopup balance successfully";
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
					insertTransactionRecord(transactionRecord);
					agentRequest.status = AgentRequest.STATUS_SUCCESS;
					agentRequest.dealer_id = dealerInfo.id;
					agentRequest.result_code = AgentRequest.RC_ADD_BALANCE_SUCCESS;
					agentRequest.transaction_id = transactionRecord.id;
					updateAgentRequest(agentRequest);
					logInfo(agentRequest.getRespString());
					
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_ADD_BALANCE_SUCCESS")
							.replaceAll("<AMOUNT>", ""+agentRequest.balance_add_amount)
							.replaceAll("<TRANS_ID>", "RDA-"+transactionRecord.id)
							.replaceAll("<BALANCE>", ""+dealerInfo.balance);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_ADD_BALANCE_SUCCESS")
							.replaceAll("<AMOUNT>", ""+agentRequest.balance_add_amount)
							.replaceAll("<TRANS_ID>", "RDA-"+transactionRecord.id)
							.replaceAll("<BALANCE>", ""+dealerInfo.balance);
					sendSms(requestInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_ADD_BALANCE, transactionRecord.id);
					
					listRequestProcessing.remove(requestInfo.msisdn);
			}
		}
	}

	private void updateDealer(AgentRequest agentRequest) {
		// TODO Auto-generated method stub
		while(true){
			try {
				connection.updateDealer(agentRequest);
				return;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logError("updateDealer error: "+MySQLConnection.getSQLExceptionString(e));
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				Connect();
			}
		}
	}

	private void OnCreateDealer(RequestInfo requestInfo) {
		// TODO Auto-generated method stub
		DealerInfo dealerInfo = null;
		DealerInfo parentInfo = null;
		AgentRequest agentRequest = requestInfo.agentRequest;
		try {
			dealerInfo = connection.getDealerInfo(requestInfo.msisdn);
			if(agentRequest.dealer_parent_id>0)
				parentInfo = connection.getDealerInfo(agentRequest.dealer_parent_id);
			requestInfo.dealerInfo = dealerInfo;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(dealerInfo!=null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DEALER_EXISTS;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			logInfo("Create Dealer: msisdn:"+requestInfo.msisdn +"; error: Number is using service");
		}
		else{
			if(agentRequest.dealer_parent_id>0&&parentInfo==null){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_PARENT_DEALER_NOT_FOUND;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				logInfo("Create sub-dealer: msisdn:"+requestInfo.msisdn +"; error: ParentID is not valid");
				return;
			}
			AgentInfo agentInitInfo = null;
			try {
				agentInitInfo = connection.getAgentInfo(agentRequest.agent_id);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				isConnected = false;
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
				logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			if(agentInitInfo == null){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_AGENT_INIT_NOT_FOUND;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			AgentInfo agentApprovedInfo = null;
			try {
				agentApprovedInfo = connection.getAgentInfo(agentRequest.agent_approved_id);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				isConnected = false;
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
				logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			if(agentApprovedInfo == null){
				agentRequest.status = AgentRequest.STATUS_FAILED;
				agentRequest.result_code = AgentRequest.RC_AGENT_APPROVED_NOT_FOUND;
				logError(agentRequest.getRespString());
				updateAgentRequest(agentRequest);
				listRequestProcessing.remove(requestInfo.msisdn);
				return;
			}
			
			agentRequest.balance_add_amount = agentRequest.addBalanceInfo.cash_value+agentRequest.addBalanceInfo.commision_value;
			dealerInfo = new DealerInfo();
			dealerInfo.active = 1;
			dealerInfo.address = agentRequest.dealer_address;
			dealerInfo.agent_init = agentInitInfo.user_name;
			dealerInfo.agent_init_id = agentInitInfo.id;
			dealerInfo.agent_approved = agentApprovedInfo.user_name;
			dealerInfo.agent_approved_id = agentApprovedInfo.id;
			dealerInfo.balance = agentRequest.balance_add_amount;
			dealerInfo.birth_date = agentRequest.dealer_birthdate;
			dealerInfo.id_card_number = agentRequest.dealer_id_card_number;
			dealerInfo.msisdn = agentRequest.dealer_msisdn;
			dealerInfo.name = agentRequest.dealer_name;
			dealerInfo.parent_id = agentRequest.dealer_parent_id;
			dealerInfo.pin_code = genRandPinCode();
			dealerInfo.province_register = agentRequest.option_dealer_province_code>0?agentRequest.option_dealer_province_code:agentInitInfo.province_code;
			dealerInfo.register_date = new Timestamp(System.currentTimeMillis());
			dealerInfo.web_password=agentRequest.web_password;
			dealerInfo.category=agentRequest.category;
			insertDealer(dealerInfo);

			TransactionRecord transactionRecord = createTransactionRecord();
			transactionRecord.dealer_msisdn = requestInfo.msisdn;
			transactionRecord.dealer_id = dealerInfo.id;
			transactionRecord.dealer_parent_id = dealerInfo.parent_id;
			transactionRecord.dealer_province = agentInitInfo.province_code;
			transactionRecord.balance_before = 0;
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.type = dealerInfo.parent_id>0?TransactionRecord.TRANS_TYPE_CREATE_SUB_DEALER:TransactionRecord.TRANS_TYPE_CREATE_DEALER;
			transactionRecord.balance_changed_amount = agentRequest.balance_add_amount;
			transactionRecord.transaction_amount_req = agentRequest.balance_add_amount;
			transactionRecord.agent = agentInitInfo.user_name;
			transactionRecord.agent_id = agentInitInfo.id;
			transactionRecord.approved = agentApprovedInfo.user_name;
			transactionRecord.approved_id = agentApprovedInfo.id;
			transactionRecord.addBalanceInfo = agentRequest.addBalanceInfo;
			transactionRecord.result_description = "Register ETopup service successfully";
			transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			insertTransactionRecord(transactionRecord);
			agentRequest.status = AgentRequest.STATUS_SUCCESS;
			agentRequest.dealer_id = dealerInfo.id;
			agentRequest.result_code = dealerInfo.parent_id>0?AgentRequest.RC_REGISTER_SUB_DEALER_SUCCESS:AgentRequest.RC_REGISTER_DEALER_SUCCESS;
			agentRequest.transaction_id = transactionRecord.id;
			updateAgentRequest(agentRequest);
			logInfo(agentRequest.getRespString());
			if(dealerInfo.parent_id>0){
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REGISTER_SUB_DEALER_SUCCESS")
						.replaceAll("<PIN>", dealerInfo.pin_code)
						.replaceAll("<DEALER_NUMBER>", ""+parentInfo.msisdn.replaceFirst("856", "0"));
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REGISTER_SUB_DEALER_SUCCESS")
						.replaceAll("<PIN>", dealerInfo.pin_code)
						.replaceAll("<DEALER_NUMBER>", ""+parentInfo.msisdn.replaceFirst("856", "0"));

				sendSms(requestInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CREATE_DEALER, transactionRecord.id);
			}
			else{
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REGISTER_DEALER_SUCCESS")
						.replaceAll("<PIN>", dealerInfo.pin_code);
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REGISTER_DEALER_SUCCESS")
						.replaceAll("<PIN>", dealerInfo.pin_code);

				sendSms(requestInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CREATE_DEALER, transactionRecord.id);

			}
			// send web user:
			String contentWebNotify = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REGISTER_DEALER_NOTIFY_WEB_USER")
					.replaceAll("<DEALER>", agentRequest.dealer_msisdn.replaceFirst("856", "0"))
					.replaceAll("<WEB_PASSWORD>", ""+ agentRequest.web_password);
			MTRecord mtRecord = new MTRecord(agentRequest.dealer_msisdn, contentWebNotify, SmsTypes.SMS_TYPE_CREATE_DEALER, transactionRecord.id);
			GlobalVars.insertSmsMTReqProcess.queueInsertMTReq.enqueue(mtRecord);
			logInfo("SendSms: msisdn:" + agentRequest.dealer_msisdn + "; content:" + contentWebNotify);
			listRequestProcessing.remove(requestInfo.msisdn);
		}
	}

	private void insertDealer(DealerInfo dealerInfo) {
		// TODO Auto-generated method stub
		while(true){
			try {
				connection.insertDealer(dealerInfo);
				return;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logError("insertDealer error: "+MySQLConnection.getSQLExceptionString(e));
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				Connect();
			}
		}
	}
	public void onRefund(RequestInfo requestInfo){
	    
       TransactionRecord old_transactionRecord = null;
       AgentRequest agentRequest = requestInfo.agentRequest;
        try {
            old_transactionRecord = connection.getTransactionRecord(agentRequest.refund_transaction_id);
            requestInfo.old_transactionRecord=old_transactionRecord;
            
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            isConnected = false;
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            return;
        }
        if(old_transactionRecord==null){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_TRANSACTION_NOT_FOUND;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: transaction not found.");
            return;
        }
        if( old_transactionRecord.status !=TransactionRecord.TRANS_STATUS_SUCCESS){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_TRANSACTION_NOT_SUCCESS;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: refund for transaction not success.");
            return;
        }
        if( old_transactionRecord.refund_status==TransactionRecord.TRANS_REFUNDED_STATUS && old_transactionRecord.type!=TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_TRANSACTION_REFUNDED;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: refund for transaction refuned.");
            return;
        }
        if( agentRequest.refund_amount > Math.abs(old_transactionRecord.balance_changed_amount)){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_AMOUNT_GREATER_THAN_TRANSACTION_AMOUNT;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: refund amount is greater than balance_changed_amount.");
            return;
        }
        DealerInfo dealerInfo=null;
        try {
            dealerInfo = connection.getDealerInfo(old_transactionRecord.dealer_id);
        } catch (SQLException e) {
            isConnected = false;
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            return;
        }
        if(dealerInfo==null){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_DEALER_NOT_FOUND;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: dealer not found.");
            return;
        }
        requestInfo.dealerInfo=dealerInfo;
        TransactionRecord transactionRecord = createTransactionRecord();
        requestInfo.transactionRecord= transactionRecord;

        transactionRecord.dealer_msisdn = old_transactionRecord.dealer_msisdn;
        transactionRecord.dealer_id = old_transactionRecord.dealer_id;
        transactionRecord.balance_before = dealerInfo.balance;
        transactionRecord.recharge_msidn= requestInfo.old_transactionRecord.recharge_msidn;
       // transactionRecord.balance_changed_amount = -1*old_transactionRecord.balance_changed_amount;
       // transactionRecord.recharge_msidn = old_transactionRecord.recharge_msidn;
        //transactionRecord.recharge_value = rechargeCmd.amount;
        transactionRecord.agent = agentRequest.agent_username;
        transactionRecord.agent_id = agentRequest.agent_id;
        transactionRecord.addBalanceInfo = agentRequest.addBalanceInfo;
        transactionRecord.refund_transaction_id=old_transactionRecord.id;
        transactionRecord.transaction_amount_req=agentRequest.refund_amount;
        agentRequest.transaction_id=transactionRecord.id;
        
        if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_RECHARGE){
            transactionRecord.type = TransactionRecord.TRANS_TYPE_REFUND_RECHARGE;
            onRefundRecharge(requestInfo);
        }else if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
            transactionRecord.type = TransactionRecord.TRANS_TYPE_REFUND_RECHARGE;
            onRefundBatchRecharge(requestInfo);
        }else if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_MOVE_STOCK){
            transactionRecord.type = TransactionRecord.TRANS_TYPE_REFUND_MOVE_STOCK;
            onRefundMoveStock(requestInfo);
        }else if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_ADD_BALANCE){
            transactionRecord.type = TransactionRecord.TRANS_TYPE_REFUND_ADD_BALANCE;
            onRefundAddBalance(requestInfo);
        }else{
           
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Refund transaction have to be recharge|move stock|add balance";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_TRANSACTION_TYPE_NOT_VALID;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Get SubInfo Failed");
            return;
        }
        
	}
	private void onRefundRecharge(RequestInfo requestInfo){
	    TransactionRecord old_transactionRecord=requestInfo.old_transactionRecord;
	    logInfo("On refund batch recharge for agentRequest: id="+requestInfo.agentRequest.id+ ", TransactionRecord: id="+old_transactionRecord.id+", dealder_msisdn="+old_transactionRecord.dealer_msisdn);
        GetSubInfoCmd getSubInfoCmd = new GetSubInfoCmd();
        getSubInfoCmd.msisdn = requestInfo.msisdn;
        getSubInfoCmd.rechargeMsisdn=old_transactionRecord.recharge_msidn;
        getSubInfoCmd.transactionId = getPaymentGWTransactionId();
        getSubInfoCmd.reqDate = new Date(System.currentTimeMillis());
        getSubInfoCmd.token = token;
        getSubInfoCmd.queueResp = queuePaymentGWResp;
        logInfo(getSubInfoCmd.getReqString());
        GlobalVars.paymentGWInterface.queueUserRequest.enqueue(getSubInfoCmd);
	}

    private void onRefundBatchRecharge(RequestInfo requestInfo) {
       
        AgentRequest agentRequest=requestInfo.agentRequest;
        TransactionRecord old_transactionRecord=requestInfo.old_transactionRecord;
        TransactionRecord transactionRecord=requestInfo.transactionRecord;
        logInfo("On refund batch recharge for agentRequest: id="+agentRequest.id+ ", TransactionRecord: id="+old_transactionRecord.id+", dealder_msisdn="+old_transactionRecord.dealer_msisdn);
        BatchRechargeElement batchRechargeElement=null;
        try {
            batchRechargeElement= connection.getRefundBatchRechargeElement(old_transactionRecord.batch_recharge_id,agentRequest.refund_msisdn);
            requestInfo.batchRechargeElement=batchRechargeElement;
        } catch (SQLException e) {
            isConnected = false;
            logError(MySQLConnection.getSQLExceptionString(e));
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "CONTENT_DB_CONNECTION_ERROR";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: CONTENT_DB_CONNECTION_ERROR");
            return;
        }
        if(batchRechargeElement ==null){
            
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Refund batch recharge msisdb not found";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_BATCH_RECHARGE_SUBSCRIBER_NOT_FOUND;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; Batch recharge list not found");
            return;
        }
        if( agentRequest.refund_amount > batchRechargeElement.recharge_value){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_AMOUNT_GREATER_THAN_RECHARGE_AMOUNT;
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: refund amount is greater than balance_changed_amount.");
            return;
        }
        
        old_transactionRecord.recharge_msidn=batchRechargeElement.recharge_msisdn;
        transactionRecord.recharge_msidn=batchRechargeElement.recharge_msisdn;
        transactionRecord.batch_recharge_id=old_transactionRecord.batch_recharge_id;
        onRefundRecharge(requestInfo);
    }
    private void onRefundMoveStock(RequestInfo requestInfo){
        //TRANS_TYPE_MOVE_STOCK
        AgentRequest agentRequest=requestInfo.agentRequest;
        TransactionRecord old_transactionRecord=requestInfo.old_transactionRecord;
        TransactionRecord transactionRecord=requestInfo.transactionRecord;
        DealerInfo receiverInfo = null;
        try {
            receiverInfo = connection.getDealerInfo(old_transactionRecord.partner_msisdn);
           // moveStockCmd.receiverInfo = receiverInfo;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            isConnected = false;  
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Connect Db error.";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Get SubInfo Failed");
            return;
        }
        if( receiverInfo==null){
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Not foud receiver.";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_MOVE_STOCK_RECEIVER_NOT_FOUND;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Not foud receiver.");
            return;
        }
        
        transactionRecord.partner_msisdn = receiverInfo.msisdn;
        transactionRecord.partner_id = receiverInfo.id;
        transactionRecord.partner_balance_before = receiverInfo.balance;
        
        MoveStockCmd moveStockCmd=new MoveStockCmd();
        moveStockCmd.dealerInfo=receiverInfo; // we reverse 
        moveStockCmd.receiverInfo=requestInfo.dealerInfo; 

        
        if( receiverInfo.balance>= Config.MULTIPLIER){
            long refundAmount=agentRequest.refund_amount;
            if(  refundAmount >receiverInfo.balance  ){
                refundAmount= (receiverInfo.balance/Config.MULTIPLIER)*Config.MULTIPLIER;
            }
            moveStockCmd.amount=(int)refundAmount;
            try {
                connection.moveStock(moveStockCmd);
                if(moveStockCmd.db_return_code==0){
                    
                    transactionRecord.balance_after = moveStockCmd.receiverBalanceAfter;// we reverse 
                    transactionRecord.partner_balance_after = moveStockCmd.balanceAfter;
                    transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
                    transactionRecord.result_description = "Refun Move Stock successfully";
                    transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
                    transactionRecord.balance_changed_amount=moveStockCmd.amount;
                    insertTransactionRecord(transactionRecord);
                    
                    moveStockCmd.resultCode = RequestCmd.R_OK;
                    moveStockCmd.resultString = "Move Stock successfully";
                    logInfo(moveStockCmd.getRespString());
                    
                    agentRequest.status = AgentRequest.STATUS_SUCCESS;
                    agentRequest.result_code = AgentRequest.RC_REFUND_MOVE_STOCK_SUCCESS;
                    updateAgentRequest(agentRequest);
                    listRequestProcessing.remove(requestInfo.msisdn);
                    
                    
                    String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_MOVE_STOCK_SUCCESS")
                            .replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
                            .replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
                            .replaceAll("<RECEIVER_NUMBER>", moveStockCmd.dealerInfo.msisdn)
                            .replaceAll("<BALANCE>", ""+moveStockCmd.balanceAfter)
                            .replaceAll("<TRANS_ID>", "MS-"+requestInfo.old_transactionRecord.id);
                    String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_MOVE_STOCK_SUCCESS")
                            .replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
                            .replaceAll("<RECEIVER_NUMBER>", moveStockCmd.dealerInfo.msisdn)
                             .replaceAll("<BALANCE>", ""+moveStockCmd.balanceAfter)
                            .replaceAll("<TRANS_ID>", "MS-"+requestInfo.old_transactionRecord.id);
                    sendSms(requestInfo.dealerInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_REFUND_MOVE_STOCK, transactionRecord.id);
                    
                    String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_RECEIVER_MOVE_STOCK_SUCCESS_NOTIFY")
                            .replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
                            .replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
                            .replaceAll("<TRANS_ID>", "MS-"+requestInfo.old_transactionRecord.id)
                            .replaceAll("<DEALER>", old_transactionRecord.dealer_msisdn.replaceFirst("856", "0"));
                    String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_RECEIVER_MOVE_STOCK_SUCCESS_NOTIFY")
                            .replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
                            .replaceAll("<TRANS_ID>", "MS-"+requestInfo.old_transactionRecord.id)
                            .replaceAll("<DEALER>", old_transactionRecord.dealer_msisdn.replaceFirst("856", "0"));
                    sendSms(receiverInfo.msisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_REFUND_MOVE_STOCK, transactionRecord.id);
                    
                    try {
                        requestInfo.old_transactionRecord.refund_status=TransactionRecord.TRANS_REFUNDED_STATUS;
                        connection.updateTransactionRecord(requestInfo.old_transactionRecord);
                    }catch (SQLException e) {
                        e.printStackTrace();
                        isConnected = false;
                        logError(MySQLConnection.getSQLExceptionString(e));
                    } catch (Exception e) {
                        e.printStackTrace();
                        logError(e.getMessage());
                    }
                }
                else{
                    transactionRecord.recharge_sub_type = 0;
                    transactionRecord.balance_after =   transactionRecord.balance_before;
                    transactionRecord.result_description = "Execute SQL function failed";
                    transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
                    transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
                    insertTransactionRecord(transactionRecord);
                    
                    agentRequest.status = AgentRequest.STATUS_FAILED;
                    agentRequest.result_code = AgentRequest.RC_CALL_MOVE_STOCK_DB_FUNCTION_ERROR;
                    updateAgentRequest(agentRequest);
                    listRequestProcessing.remove(requestInfo.msisdn);
                    logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Get SubInfo Failed");
                    return;
                }
            } catch (SQLException e) {
                
                isConnected = false;  
                transactionRecord.recharge_sub_type = 0;
                transactionRecord.balance_after =   transactionRecord.balance_before;
                transactionRecord.result_description = "Connect Db error.";
                transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
                transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
                insertTransactionRecord(transactionRecord);
                
                agentRequest.status = AgentRequest.STATUS_FAILED;
                agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
                updateAgentRequest(agentRequest);
                listRequestProcessing.remove(requestInfo.msisdn);
                logInfo("Refund transaction : id:"+requestInfo.agentRequest.id +"; error: Get SubInfo Failed");
                return;

            }
        }else{
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Receiver balance is not enough.";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_MOVE_STOCK_RECEIVER_BALANCE_NOT_ENOUGH;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.id +"; error: Receiver balance is not enough.");
            return;
        }
        
        
    }
    private void onRefundAddBalance(RequestInfo requestInfo){
    	DealerInfo dealerInfo = null;
		AgentRequest agentRequest = requestInfo.agentRequest;
		try {
			dealerInfo = connection.getDealerInfo(requestInfo.msisdn);
			requestInfo.dealerInfo = dealerInfo;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DB_CONNECTION_ERROR;
			logError(agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(dealerInfo==null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_code = AgentRequest.RC_DEALER_NOT_FOUND;
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			logInfo("AddBalance: msisdn:"+requestInfo.msisdn +"; error: Number is not a Dealer");
			return;
		}
		
        requestInfo.dealerInfo = dealerInfo;
        TransactionRecord old_transactionRecord=requestInfo.old_transactionRecord;
        TransactionRecord transactionRecord=requestInfo.transactionRecord;
        
        long refundAmount=old_transactionRecord.balance_changed_amount;
        
        if(  refundAmount > dealerInfo.balance  ){
            refundAmount= ( dealerInfo.balance/Config.MULTIPLIER)*Config.MULTIPLIER;
        }
		
        agentRequest.balance_add_amount = -1*refundAmount;
        agentRequest.dealer_id = dealerInfo.id;
        updateDealer(agentRequest);
        
        transactionRecord.balance_after = dealerInfo.balance-refundAmount; 
        transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
        transactionRecord.result_description = "Refund add balance successfully";
        transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
        transactionRecord.balance_changed_amount=-1*refundAmount;
        insertTransactionRecord(transactionRecord);

        String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_ADD_BALANCE_SUCCESS")
                .replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
                .replaceAll("<AMOUNT>", ""+refundAmount)
                .replaceAll("<BALANCE>", ""+transactionRecord.balance_after)
                .replaceAll("<TRANS_ID>", "RDA-"+requestInfo.old_transactionRecord.id);
        String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_ADD_BALANCE_SUCCESS")
                .replaceAll("<AMOUNT>", ""+refundAmount)
                .replaceAll("<BALANCE>", ""+transactionRecord.balance_after)
                .replaceAll("<TRANS_ID>", "RDA-"+requestInfo.old_transactionRecord.id);
        sendSms(requestInfo.dealerInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_REFUND_MOVE_STOCK, transactionRecord.id);
        
        
        try {
            requestInfo.old_transactionRecord.refund_status=TransactionRecord.TRANS_REFUNDED_STATUS;
            connection.updateTransactionRecord(requestInfo.old_transactionRecord);
        }catch (SQLException e) {
            isConnected = false;
            logError(MySQLConnection.getSQLExceptionString(e));
        } catch (Exception e) {
            e.printStackTrace();
            logError(e.getMessage());
        }
        
        agentRequest.status = AgentRequest.STATUS_SUCCESS;
        agentRequest.result_code = AgentRequest.RC_REFUND_ADD_BALANCE_SUCCESS;
        updateAgentRequest(agentRequest);
        listRequestProcessing.remove(requestInfo.msisdn);
        
    }
    
    @SuppressWarnings("unused")
	private void refundBatchRechargeGetSubInfoElement(RequestInfo requestInfo) {
        BatchRechargeCmd batchRechargeCmd=(BatchRechargeCmd)requestInfo.dealerRequest.requestCmd;
        GetSubInfoCmd getSubInfoCmd = new GetSubInfoCmd();
        getSubInfoCmd.msisdn = requestInfo.msisdn;
        getSubInfoCmd.rechargeMsisdn=batchRechargeCmd.currentBatchRechargeElement.recharge_msisdn;
        getSubInfoCmd.transactionId = getPaymentGWTransactionId();
        getSubInfoCmd.reqDate = new Date(System.currentTimeMillis());
        getSubInfoCmd.token = token;
        getSubInfoCmd.queueResp = queuePaymentGWResp;
        logInfo(getSubInfoCmd.getReqString());
        GlobalVars.paymentGWInterface.queueUserRequest.enqueue(getSubInfoCmd);
    }
	private String genRandPinCode() {
		// TODO Auto-generated method stub
		Random rand = new Random();
		int value = rand.nextInt(10000);
		while(value%1000==0){
			value = rand.nextInt(10000);
		}
		return String.format("%04d", value);
	}

	private void updateAgentRequest(AgentRequest agentRequest) {
		// TODO Auto-generated method stub
		try {
			connection.updateAgentRequest(agentRequest);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logError("Update "+agentRequest.getRespString()+"; error:"+MySQLConnection.getSQLExceptionString(e));
		}
	}

	private void OnPaymentPostpaidCmdResp(PaymentPostpaidCmd paymentPostpaidCmdResp) {
		// TODO Auto-generated method stub
		logInfo(paymentPostpaidCmdResp.getRespString());
		RequestInfo requestInfo = listRequestProcessing.get(paymentPostpaidCmdResp.msisdn);
		if(requestInfo.dealerRequest.requestCmd instanceof RechargeCmd){
			OnRechargePaymentPostpaidCmdResp(paymentPostpaidCmdResp);
		}
		else{
			OnBatchRechargePaymentPostpaidCmdResp(paymentPostpaidCmdResp);
		}
	}
	
	private void OnRechargePaymentPostpaidCmdResp(PaymentPostpaidCmd paymentPostpaidCmdResp) {
		// TODO Auto-generated method stub
		RequestInfo requestInfo = listRequestProcessing.get(paymentPostpaidCmdResp.msisdn);
		RechargeCmd rechargeCmd = (RechargeCmd) requestInfo.dealerRequest.requestCmd;
//		DealerInfo dealerInfo = rechargeCmd.dealerInfo;
		DealerRequest dealerRequest = requestInfo.dealerRequest;
		TransactionRecord transactionRecord = requestInfo.transactionRecord;
		if(paymentPostpaidCmdResp.resultCode==PaymentGWResultCode.RC_PAYMENT_POSTPAID_SUCCESS){
			try {
				connection.deductBalance(rechargeCmd);
				if(rechargeCmd.db_return_code==0){
					DelayRecharge delayRecharge = new DelayRecharge();
					delayRecharge.amount = paymentPostpaidCmdResp.amount;
					listDelayRecharges.put(paymentPostpaidCmdResp.msisdn+"_"+paymentPostpaidCmdResp.rechargeMsisdn, delayRecharge);
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.balance_changed_amount = -1*rechargeCmd.amount;
					transactionRecord.balance_after = rechargeCmd.balanceAfter;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
					transactionRecord.result_description = "Payment Postpaid subscriber success";
					insertTransactionRecord(transactionRecord);
					rechargeCmd.resultCode = RequestCmd.R_OK;
					rechargeCmd.resultString = transactionRecord.result_description;
					logInfo(rechargeCmd.getRespString());
					if(("856"+rechargeCmd.rechargeMsisdn).equals(rechargeCmd.msisdn)){
						String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECHARGE_OWNER_SUCCESS")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<BALANCE>", ""+rechargeCmd.balanceAfter)
								.replaceAll("<TRANS_ID>", "RS-"+transactionRecord.id);
						String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECHARGE_OWNER_SUCCESS").replaceAll("<AMOUNT>", ""+rechargeCmd.amount);
						sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						dealerRequest.result = "CONTENT_RECHARGE_OWNER_SUCCESS";
					}
					else{
						String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECHARGE_OTHER_SUCCESS")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<RECEIVER_NUMBER>", rechargeCmd.rechargeMsisdn)
								.replaceAll("<BALANCE>", ""+rechargeCmd.balanceAfter)
								.replaceAll("<TRANS_ID>", "RS-"+transactionRecord.id);
						String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECHARGE_OTHER_SUCCESS")
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<RECEIVER_NUMBER>", rechargeCmd.rechargeMsisdn);
						
						sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						
						String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<DEALER_NUMBER> ", rechargeCmd.msisdn.replaceFirst("856", "0"));
						String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<DEALER_NUMBER> ", rechargeCmd.msisdn.replaceFirst("856", "0"));
						
						sendSms("856"+rechargeCmd.rechargeMsisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						dealerRequest.result = "CONTENT_RECHARGE_OTHER_SUCCESS";
					}
					updateDealerRequest(requestInfo.dealerRequest);
					createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_SUCCESS);
					listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				}
				else{
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.balance_after = rechargeCmd.balanceAfter;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = "Execute SQL function failed";
					insertTransactionRecord(transactionRecord);
					rechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
					rechargeCmd.resultString = "Execute SQL function failed";
					logError(rechargeCmd.getRespString());
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_DEDUCT_FUNCTION_ERROR");
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_DEDUCT_FUNCTION_ERROR");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
					dealerRequest.result = "CONTENT_DB_DEDUCT_FUNCTION_ERROR";
					updateDealerRequest(requestInfo.dealerRequest);
					createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
					listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				isConnected = false;
				rechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
				rechargeCmd.resultString = MySQLConnection.getSQLExceptionString(e);
				logError(rechargeCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
				sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, 0);
				requestInfo.dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
				requestInfo.dealerRequest.dealer_id = requestInfo.dealerInfo.id;
				updateDealerRequest(requestInfo.dealerRequest);
				createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
				listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				return;
			}
		}
		else{
			transactionRecord.balance_after = requestInfo.dealerInfo.balance;
			transactionRecord.type = TransactionRecord.TRANS_TYPE_RECHARGE;
			transactionRecord.recharge_msidn = rechargeCmd.rechargeMsisdn;
			transactionRecord.recharge_value = rechargeCmd.amount;
			rechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			rechargeCmd.resultString = "Payment Postpaid subscriber failed";
			transactionRecord.result_description = rechargeCmd.resultString;
			logInfo(requestInfo.dealerRequest.requestCmd.getRespString());
			requestInfo.dealerRequest.result = "CONTENT_PAYMENT_POSTPAID_FAILED";
			transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;

			insertTransactionRecord(transactionRecord);
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_PAYMENT_POSTPAID_FAILED");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_PAYMENT_POSTPAID_FAILED");
			sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
			updateDealerRequest(requestInfo.dealerRequest);
			createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
			listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
			
		}
	}

	private void OnBatchRechargePaymentPostpaidCmdResp(PaymentPostpaidCmd paymentPostpaidCmdResp) {
		// TODO Auto-generated method stub
		RequestInfo requestInfo = listRequestProcessing.get(paymentPostpaidCmdResp.msisdn);
		BatchRechargeCmd batchRechargeCmd = (BatchRechargeCmd) requestInfo.dealerRequest.requestCmd;
//		DealerInfo dealerInfo = batchRechargeCmd.dealerInfo;
		DealerRequest dealerRequest = requestInfo.dealerRequest;
		TransactionRecord transactionRecord = requestInfo.transactionRecord;
		BatchRechargeElement batchRechargeElement = batchRechargeCmd.currentBatchRechargeElement;
		if(paymentPostpaidCmdResp.resultCode==PaymentGWResultCode.RC_PAYMENT_POSTPAID_SUCCESS){
            logInfo("PaymentPostpaid succes for:"+batchRechargeElement.toString());
            batchRechargeCmd.recharge_success++;
            batchRechargeCmd.recharge_success_amount+=paymentPostpaidCmdResp.amount;
            batchRechargeElement.status = BatchRechargeElement.STATUS_SUCCESS;
            batchRechargeElement.result_code = paymentPostpaidCmdResp.resultCode;
            batchRechargeElement.result_string = paymentPostpaidCmdResp.resultString;
            updateBatchRechargeElement(batchRechargeElement); 
			transactionRecord.balance_changed_amount = transactionRecord.balance_changed_amount-batchRechargeElement.recharge_value;
			try {
				connection.deductBalance(batchRechargeElement);
				if(batchRechargeCmd.currentBatchRechargeElement.db_return_code==0){
				    batchRechargeCmd.lastBalanceAfter=batchRechargeElement.balanceAfter;
				    
					logInfo(batchRechargeCmd.getRespString());
					String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<DATE_TIME>", getDateTimeFormated( new Timestamp(System.currentTimeMillis())))
								.replaceAll("<AMOUNT>", ""+paymentPostpaidCmdResp.amount)
								.replaceAll("<DEALER_NUMBER> ", dealerRequest.msisdn.replaceFirst("856", "0"));
					String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<AMOUNT>", ""+ paymentPostpaidCmdResp.amount)
								.replaceAll("<DEALER_NUMBER> ",dealerRequest.msisdn.replaceFirst("856", "0"));
						
					sendSms("856"+batchRechargeCmd.currentBatchRechargeElement.recharge_msisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
					transactionRecord.balance_after=batchRechargeElement.balanceAfter;  // save for create recharge cdr
					createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
					if(batchRechargeCmd.batchRechargeElements.isEmpty()){
		                onBatchRechargeDone(requestInfo);
		            }
		            else{
		                batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
		                batchRechargeGetSubInfoForElement(requestInfo);
		            }
				}
				else{
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
//					transactionRecord.balance_after = batchRechargeCmd.balanceAfter;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = "Execute SQL function failed";
					insertTransactionRecord(transactionRecord);
					batchRechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
					batchRechargeCmd.resultString = "Execute SQL function failed";
					logError(batchRechargeCmd.getRespString());
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_DEDUCT_FUNCTION_ERROR");
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_DEDUCT_FUNCTION_ERROR");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
					dealerRequest.result = "CONTENT_DB_DEDUCT_FUNCTION_ERROR";
					updateDealerRequest(requestInfo.dealerRequest);
					createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
					listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				isConnected = false;
				batchRechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
				batchRechargeCmd.resultString = MySQLConnection.getSQLExceptionString(e);
				logError(batchRechargeCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
				sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, 0);
				requestInfo.dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
				requestInfo.dealerRequest.dealer_id = requestInfo.dealerInfo.id;
				updateDealerRequest(requestInfo.dealerRequest);
				createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
				listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				return;
			}
		}
		else{
		    
	        batchRechargeCmd.recharge_failed++;
            batchRechargeElement.status = BatchRechargeElement.STATUS_FAILED;
            batchRechargeElement.result_code = paymentPostpaidCmdResp.resultCode;
            batchRechargeElement.result_string = paymentPostpaidCmdResp.resultString;
            updateBatchRechargeElement(batchRechargeElement);
            createRechargeCdrRecordForOnPaymentPostpaidCmd(paymentPostpaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
            if(batchRechargeCmd.batchRechargeElements.isEmpty()){
                onBatchRechargeDone(requestInfo);
            }
            else{
                batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
                batchRechargeGetSubInfoForElement(requestInfo);
            }
		}
	}
	
    private void OnTopupPrepaidCmdResp(TopupPrepaidCmd topupPrepaidCmdResp) {
        logInfo(topupPrepaidCmdResp.getRespString());
        RequestInfo requestInfo = listRequestProcessing.get(topupPrepaidCmdResp.msisdn);
        if(requestInfo.dealerRequest.requestCmd instanceof RechargeCmd){
            OnRechargeTopupPrepaidCmdResp(topupPrepaidCmdResp);
        }
        else{
            OnBatchRechargeTopupPrepaidCmdResp(topupPrepaidCmdResp);
        }
    }
	
	private void OnRechargeTopupPrepaidCmdResp(TopupPrepaidCmd topupPrepaidCmdResp) {
		// TODO Auto-generated method stub
		logInfo(topupPrepaidCmdResp.getRespString());
		RequestInfo requestInfo = listRequestProcessing.get(topupPrepaidCmdResp.msisdn);
		RechargeCmd rechargeCmd = (RechargeCmd) requestInfo.dealerRequest.requestCmd;
		DealerRequest dealerRequest = requestInfo.dealerRequest;
		if(topupPrepaidCmdResp.resultCode==PaymentGWResultCode.RC_TOPUP_PREPAID_SUCCESS){
			TransactionRecord transactionRecord = requestInfo.transactionRecord;
			try {
				connection.deductBalance(rechargeCmd);
				if(rechargeCmd.db_return_code==0){
					DelayRecharge delayRecharge = new DelayRecharge();
					delayRecharge.amount = topupPrepaidCmdResp.amount;
					listDelayRecharges.put(topupPrepaidCmdResp.msisdn+"_"+topupPrepaidCmdResp.rechargeMsisdn, delayRecharge);
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.balance_changed_amount = -1*rechargeCmd.amount;
					transactionRecord.balance_after = rechargeCmd.balanceAfter;
					transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_PREPAID;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
					transactionRecord.result_description = "Topup Prepaid subscriber success";
					insertTransactionRecord(transactionRecord);
					rechargeCmd.resultCode = RequestCmd.R_OK;
					rechargeCmd.resultString = transactionRecord.result_description;
					logInfo(rechargeCmd.getRespString());
					if(("856"+rechargeCmd.rechargeMsisdn).equals(rechargeCmd.msisdn)){
						String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECHARGE_OWNER_SUCCESS")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<BALANCE>", ""+rechargeCmd.balanceAfter)
								.replaceAll("<TRANS_ID>", ""+transactionRecord.id);
						String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECHARGE_OWNER_SUCCESS")
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount);
						sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						dealerRequest.result = "CONTENT_RECHARGE_OWNER_SUCCESS";
					}
					else{
						String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECHARGE_OTHER_SUCCESS")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<RECEIVER_NUMBER>", rechargeCmd.rechargeMsisdn)
								.replaceAll("<BALANCE>", ""+rechargeCmd.balanceAfter)
								.replaceAll("<TRANS_ID>", ""+transactionRecord.id);
						String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECHARGE_OTHER_SUCCESS")
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<RECEIVER_NUMBER>", rechargeCmd.rechargeMsisdn);
						sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						
						String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<DEALER_NUMBER> ", rechargeCmd.msisdn.replaceFirst("856", "0"));
						String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<AMOUNT>", ""+rechargeCmd.amount)
								.replaceAll("<DEALER_NUMBER> ", rechargeCmd.msisdn.replaceFirst("856", "0"));
						sendSms("856"+rechargeCmd.rechargeMsisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						
						dealerRequest.result = "CONTENT_RECHARGE_OTHER_SUCCESS";
					}
					updateDealerRequest(requestInfo.dealerRequest);
	                createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_SUCCESS); 
					listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
					
				}
				else{
					transactionRecord.balance_after = rechargeCmd.balanceAfter;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = "Execute SQL function failed";
					insertTransactionRecord(transactionRecord);
					rechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
					rechargeCmd.resultString = "Execute SQL function failed";
					logError(rechargeCmd.getRespString());
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_DEDUCT_FUNCTION_ERROR");
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_DEDUCT_FUNCTION_ERROR");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
					dealerRequest.result = "CONTENT_DB_DEDUCT_FUNCTION_ERROR";
					updateDealerRequest(requestInfo.dealerRequest);
					insertTransactionRecord(transactionRecord);
					createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_FAILED); 
					listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
					
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				isConnected = false;
				rechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
				rechargeCmd.resultString = MySQLConnection.getSQLExceptionString(e);
				logError(rechargeCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
				sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, 0);
				requestInfo.dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
				updateDealerRequest(requestInfo.dealerRequest);
				createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_FAILED); 
				listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				return;
			}
		}
		else{
			TransactionRecord transactionRecord = requestInfo.transactionRecord;
			transactionRecord.balance_after = requestInfo.dealerInfo.balance;
			rechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			rechargeCmd.resultString = "Topup Prepaid subscriber failed";
			transactionRecord.result_description = rechargeCmd.resultString;
			logInfo(requestInfo.dealerRequest.requestCmd.getRespString());
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			insertTransactionRecord(transactionRecord);
			
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_TOPUP_PREPAID_FAILED");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_TOPUP_PREPAID_FAILED");
			sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
			requestInfo.dealerRequest.result = "CONTENT_TOPUP_PREPAID_FAILED";
			updateDealerRequest(requestInfo.dealerRequest);
			createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_RECHARGE,RechargeCdrRecord.STATUS_FAILED);
			listRequestProcessing.remove(requestInfo.dealerRequest.msisdn); 
		}
	}

    private void createRechargeCdrRecordForTopupPrepaid(TopupPrepaidCmd topupPrepaidCmdResp,int type,int result) {
        RequestInfo requestInfo = listRequestProcessing.get(topupPrepaidCmdResp.msisdn);
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        RechargeCdrRecord rechargeCdrRecord=new RechargeCdrRecord();
        rechargeCdrRecord.payment_transaction_id=topupPrepaidCmdResp.transactionId;
        rechargeCdrRecord.date_time= new Timestamp(System.currentTimeMillis());
        rechargeCdrRecord.type=type;
        rechargeCdrRecord.dealer_msisdn=transactionRecord.dealer_msisdn;
        rechargeCdrRecord.dealer_id=transactionRecord.dealer_id;
        rechargeCdrRecord.balance_changed_amount=-1*topupPrepaidCmdResp.amount;
        rechargeCdrRecord.balance_before=transactionRecord.balance_before;
        rechargeCdrRecord.balance_after=transactionRecord.balance_after;
        rechargeCdrRecord.receiver_msidn=topupPrepaidCmdResp.rechargeMsisdn;
        rechargeCdrRecord.receiver_sub_type=GetSubInfoCmd.SUBS_TYPE_PREPAID;
        rechargeCdrRecord.recharge_value=topupPrepaidCmdResp.amount;
        rechargeCdrRecord.transaction_id=transactionRecord.id;
        rechargeCdrRecord.result=result;
        rechargeCdrRecord.result_code=topupPrepaidCmdResp.resultCode;
        rechargeCdrRecord.result_description=topupPrepaidCmdResp.resultString;
        insertRechargeCdrRecord(rechargeCdrRecord);
    }
    private void createRechargeCdrRecordForOnPaymentPostpaidCmd(PaymentPostpaidCmd paymentPostpaidCmd,int type,int result) {
        RequestInfo requestInfo = listRequestProcessing.get(paymentPostpaidCmd.msisdn);
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        RechargeCdrRecord rechargeCdrRecord=new RechargeCdrRecord();
        rechargeCdrRecord.payment_transaction_id=paymentPostpaidCmd.transactionId;
        rechargeCdrRecord.date_time= new Timestamp(System.currentTimeMillis());
        rechargeCdrRecord.type=type;
        rechargeCdrRecord.dealer_msisdn=transactionRecord.dealer_msisdn;
        rechargeCdrRecord.dealer_id=transactionRecord.dealer_id;
        rechargeCdrRecord.balance_changed_amount=-1*paymentPostpaidCmd.amount;
        rechargeCdrRecord.balance_before=transactionRecord.balance_before;
        rechargeCdrRecord.balance_after=transactionRecord.balance_after;
        rechargeCdrRecord.receiver_msidn=paymentPostpaidCmd.rechargeMsisdn;
        rechargeCdrRecord.receiver_sub_type=GetSubInfoCmd.SUBS_TYPE_POSTPAID;
        rechargeCdrRecord.recharge_value=paymentPostpaidCmd.amount;
        rechargeCdrRecord.transaction_id=transactionRecord.id;
        rechargeCdrRecord.result=result;
        rechargeCdrRecord.result_code=paymentPostpaidCmd.resultCode;
        rechargeCdrRecord.result_description=paymentPostpaidCmd.resultString;
        insertRechargeCdrRecord(rechargeCdrRecord);
    }
	private void OnBatchRechargeTopupPrepaidCmdResp(TopupPrepaidCmd topupPrepaidCmdResp) {
        // TODO Auto-generated method stub
        logInfo(topupPrepaidCmdResp.getRespString());
        RequestInfo requestInfo = listRequestProcessing.get(topupPrepaidCmdResp.msisdn);
        BatchRechargeCmd batchRechargeCmd = (BatchRechargeCmd) requestInfo.dealerRequest.requestCmd;
//      DealerInfo dealerInfo = batchRechargeCmd.dealerInfo;
        DealerRequest dealerRequest = requestInfo.dealerRequest;
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        BatchRechargeElement batchRechargeElement = batchRechargeCmd.currentBatchRechargeElement;

        if(topupPrepaidCmdResp.resultCode==PaymentGWResultCode.RC_TOPUP_PREPAID_SUCCESS){
            logInfo("TopupPrepaid succes for:"+batchRechargeElement.toString());
            logInfo("PaymentPostpaid succes for:"+batchRechargeElement.toString());
            batchRechargeCmd.recharge_success++;
            batchRechargeCmd.recharge_success_amount+=topupPrepaidCmdResp.amount;
            batchRechargeElement.status = BatchRechargeElement.STATUS_SUCCESS;
            batchRechargeElement.result_code = topupPrepaidCmdResp.resultCode;
            batchRechargeElement.result_string = topupPrepaidCmdResp.resultString;
            updateBatchRechargeElement(batchRechargeElement); 
            transactionRecord.balance_changed_amount = transactionRecord.balance_changed_amount-batchRechargeElement.recharge_value;
            try {
                connection.deductBalance(batchRechargeElement);
                if(batchRechargeCmd.currentBatchRechargeElement.db_return_code==0){
                    batchRechargeCmd.lastBalanceAfter=batchRechargeElement.balanceAfter;
                    
                    logInfo(batchRechargeCmd.getRespString());
                    String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
                                .replaceAll("<DATE_TIME>", getDateTimeFormated( new Timestamp(System.currentTimeMillis())))
                                .replaceAll("<AMOUNT>", ""+topupPrepaidCmdResp.amount)
                                .replaceAll("<DEALER_NUMBER> ", dealerRequest.msisdn.replaceFirst("856", "0"));
                    String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
                                .replaceAll("<AMOUNT>", ""+topupPrepaidCmdResp.amount)
                                .replaceAll("<DEALER_NUMBER> ", dealerRequest.msisdn.replaceFirst("856", "0"));
                        
                    sendSms("856"+batchRechargeCmd.currentBatchRechargeElement.recharge_msisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
                    transactionRecord.balance_after=batchRechargeElement.balanceAfter;  // save for create recharge cdr
                    createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_SUCCESS); 
                    if(batchRechargeCmd.batchRechargeElements.isEmpty()){
                        onBatchRechargeDone(requestInfo);
                    }
                    else{
                        batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
                        batchRechargeGetSubInfoForElement(requestInfo);
                    }
                }
                else{
                    transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
//                  transactionRecord.balance_after = batchRechargeCmd.balanceAfter;
                    transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
                    transactionRecord.result_description = "Execute SQL function failed";
                    insertTransactionRecord(transactionRecord);
                    batchRechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
                    batchRechargeCmd.resultString = "Execute SQL function failed";
                    logError(batchRechargeCmd.getRespString());
                    String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_DEDUCT_FUNCTION_ERROR");
                    String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_DEDUCT_FUNCTION_ERROR");
                    sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
                    dealerRequest.result = "CONTENT_DB_DEDUCT_FUNCTION_ERROR";
                    updateDealerRequest(requestInfo.dealerRequest);
                    createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED); 
                    listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
                isConnected = false;
                batchRechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
                batchRechargeCmd.resultString = MySQLConnection.getSQLExceptionString(e);
                logError(batchRechargeCmd.getRespString());
                String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
                String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
                sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, 0);
                requestInfo.dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
                requestInfo.dealerRequest.dealer_id = requestInfo.dealerInfo.id;
                updateDealerRequest(requestInfo.dealerRequest);
                createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED); 
                listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
                return;
            }
        }
        else{
            
            batchRechargeCmd.recharge_failed++;
            batchRechargeElement.status = BatchRechargeElement.STATUS_FAILED;
            batchRechargeElement.result_code = topupPrepaidCmdResp.resultCode;
            batchRechargeElement.result_string = topupPrepaidCmdResp.resultString;
            updateBatchRechargeElement(batchRechargeElement);
            createRechargeCdrRecordForTopupPrepaid(topupPrepaidCmdResp, RechargeCdrRecord.TYPE_BATCH_RECHARGE,RechargeCdrRecord.STATUS_FAILED); 
            if(batchRechargeCmd.batchRechargeElements.isEmpty()){
                onBatchRechargeDone(requestInfo);
            }
            else{
                batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
                batchRechargeGetSubInfoForElement(requestInfo);
            }
        }
    }
	private void OnGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp) {
		// TODO Auto-generated method stub
		logInfo(getSubInfoCmdResp.getRespString());
		RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
		if( requestInfo.old_transactionRecord !=null ){
		    OnRefundGetSubInfoResp(getSubInfoCmdResp);
		    return;
		}
		if(requestInfo.dealerRequest.requestCmd instanceof RechargeCmd){
			OnRechargeGetSubInfoResp(getSubInfoCmdResp);
		}
		else{
			OnBatchRechargeGetSubInfoResp(getSubInfoCmdResp);
		}
	}
	
	private void OnRechargeGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp) {
		// TODO Auto-generated method stub
		logInfo(getSubInfoCmdResp.getRespString());
		RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
		TransactionRecord transactionRecord = requestInfo.transactionRecord;
		if(getSubInfoCmdResp.resultCode==PaymentGWResultCode.RC_GET_SUBS_INFO_SUCCESS){
			if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_PREPAID){
				transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_PREPAID;
				TopupPrepaidCmd topupPrepaidCmd = new TopupPrepaidCmd();
				topupPrepaidCmd.msisdn = getSubInfoCmdResp.msisdn;
				topupPrepaidCmd.transactionId = getPaymentGWTransactionId();
				RechargeCmd rechargeCmd = (RechargeCmd) requestInfo.dealerRequest.requestCmd;
				topupPrepaidCmd.amount = rechargeCmd.amount;
				topupPrepaidCmd.reqDate = new Date(System.currentTimeMillis());
				topupPrepaidCmd.rechargeMsisdn = getSubInfoCmdResp.rechargeMsisdn;
				topupPrepaidCmd.token = token;
				topupPrepaidCmd.queueResp = queuePaymentGWResp;
				logInfo(topupPrepaidCmd.getReqString());
				GlobalVars.paymentGWInterface.queueUserRequest.enqueue(topupPrepaidCmd);
			}
			else if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_POSTPAID){
				transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_POSTPAID;
				PaymentPostpaidCmd paymentPostpaidCmd = new PaymentPostpaidCmd();
				paymentPostpaidCmd.msisdn = getSubInfoCmdResp.msisdn;
				paymentPostpaidCmd.transactionId = getPaymentGWTransactionId();
				RechargeCmd rechargeCmd = (RechargeCmd) requestInfo.dealerRequest.requestCmd;
				paymentPostpaidCmd.amount = rechargeCmd.amount;
				paymentPostpaidCmd.reqDate = new Date(System.currentTimeMillis());
				paymentPostpaidCmd.rechargeMsisdn = getSubInfoCmdResp.rechargeMsisdn;
				paymentPostpaidCmd.token = token;
				paymentPostpaidCmd.queueResp = queuePaymentGWResp;
				logInfo(paymentPostpaidCmd.getReqString());
				GlobalVars.paymentGWInterface.queueUserRequest.enqueue(paymentPostpaidCmd);
			}
		}
		else{
			transactionRecord.recharge_sub_type = 0;
			RechargeCmd rechargeCmd = (RechargeCmd) requestInfo.dealerRequest.requestCmd;
			rechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			rechargeCmd.resultString = "Get SubInfo Failed";
			logInfo(requestInfo.dealerRequest.requestCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_GETS_SUBINFO_FAILED");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_GETS_SUBINFO_FAILED");
			sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			requestInfo.dealerRequest.result = "CONTENT_GETS_SUBINFO_FAILED";
			updateDealerRequest(requestInfo.dealerRequest);
			transactionRecord.balance_after = requestInfo.dealerInfo.balance;
			transactionRecord.result_description = rechargeCmd.resultString;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			insertTransactionRecord(transactionRecord);
			listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
		}
	}
	private void OnBatchRechargeGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp) {
		// TODO Auto-generated method stub
		logInfo(getSubInfoCmdResp.getRespString());
		RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
		BatchRechargeCmd batchRechargeCmd = (BatchRechargeCmd) requestInfo.dealerRequest.requestCmd;
        BatchRechargeElement batchRechargeElement = batchRechargeCmd.currentBatchRechargeElement;
        
		if(getSubInfoCmdResp.resultCode==PaymentGWResultCode.RC_GET_SUBS_INFO_SUCCESS){
			if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_PREPAID){
				TopupPrepaidCmd topupPrepaidCmd = new TopupPrepaidCmd();
				topupPrepaidCmd.msisdn = getSubInfoCmdResp.msisdn;
				topupPrepaidCmd.transactionId = getPaymentGWTransactionId();
				topupPrepaidCmd.amount = batchRechargeElement.recharge_value;
				topupPrepaidCmd.reqDate = new Date(System.currentTimeMillis());
				topupPrepaidCmd.rechargeMsisdn = getSubInfoCmdResp.rechargeMsisdn;
				topupPrepaidCmd.token = token;
				topupPrepaidCmd.queueResp = queuePaymentGWResp;
				logInfo(topupPrepaidCmd.getReqString());
				GlobalVars.paymentGWInterface.queueUserRequest.enqueue(topupPrepaidCmd);
			}
			else if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_POSTPAID){
				PaymentPostpaidCmd paymentPostpaidCmd = new PaymentPostpaidCmd();
				paymentPostpaidCmd.msisdn = getSubInfoCmdResp.msisdn;
				paymentPostpaidCmd.transactionId = getPaymentGWTransactionId();
				paymentPostpaidCmd.amount = batchRechargeElement.recharge_value;
				paymentPostpaidCmd.reqDate = new Date(System.currentTimeMillis());
				paymentPostpaidCmd.rechargeMsisdn = getSubInfoCmdResp.rechargeMsisdn;
				paymentPostpaidCmd.token = token;
				paymentPostpaidCmd.queueResp = queuePaymentGWResp;
				logInfo(paymentPostpaidCmd.getReqString());
				GlobalVars.paymentGWInterface.queueUserRequest.enqueue(paymentPostpaidCmd);
			}
		}
		else{
			batchRechargeCmd.recharge_failed++;
			batchRechargeElement.status = BatchRechargeElement.STATUS_FAILED;
			batchRechargeElement.result_code = getSubInfoCmdResp.resultCode;
			batchRechargeElement.result_string = getSubInfoCmdResp.resultString;
			updateBatchRechargeElement(batchRechargeElement);
			
			if(batchRechargeCmd.batchRechargeElements.isEmpty()){
			    onBatchRechargeDone(requestInfo);
			}
			else{
				batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
				batchRechargeGetSubInfoForElement(requestInfo);
			}
		}
	}

    private void onBatchRechargeDone(RequestInfo requestInfo) {
        DealerRequest dealerRequest = requestInfo.dealerRequest;
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        transactionRecord.type = TransactionRecord.TRANS_TYPE_BATCH_RECHARGE;
        BatchRechargeCmd batchRechargeCmd = (BatchRechargeCmd) requestInfo.dealerRequest.requestCmd;
        String contentSms = "";
        String contentUssd = "";
        if (batchRechargeCmd.recharge_success <= 0) {
            logInfo("Failed for: " + batchRechargeCmd.getRespString());

            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.result_description = "Batch recharge failed.";
            transactionRecord.balance_after = requestInfo.dealerInfo.balance;

            dealerRequest.result = "CONTENT_BATCH_RECHARGE_FAIL";


            batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
            batchRechargeCmd.resultString = "Batch recharge failed.";

            contentSms = Config.smsMessageContents[Config.smsLanguage]
                    .getParam("CONTENT_BATCH_RECHARGE_FAILED");
            contentUssd = Config.ussdMessageContents[Config.smsLanguage]
                    .getParam("CONTENT_BATCH_RECHARGE_FAILED");

        } else {
            logInfo("Success for: " + batchRechargeCmd.getRespString());
            transactionRecord.result_description = "Batch recharge success.";
            transactionRecord.balance_after = batchRechargeCmd.lastBalanceAfter;

            dealerRequest.result = "CONTENT_BATCH_RECHARGE_SUCCESS";

            batchRechargeCmd.resultCode = RequestCmd.R_OK;
            batchRechargeCmd.resultString = transactionRecord.result_description;

            contentSms = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BATCH_RECHARGE_SUCCSESS");
            contentUssd = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BATCH_RECHARGE_SUCCSESS");
                
        }
        transactionRecord.balance_changed_amount = -1* batchRechargeCmd.recharge_success_amount;
        transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
        transactionRecord.batch_recharge_succes=batchRechargeCmd.recharge_success;
        transactionRecord.batch_recharge_fail=batchRechargeCmd.recharge_failed;
        insertTransactionRecord(transactionRecord);

        contentSms = contentSms
                .replaceAll("<DATE_TIME>",
                        getDateTimeFormated(transactionRecord.date_time))
                .replaceAll("<AMOUNT>",
                        "" + batchRechargeCmd.recharge_success_amount)
                .replaceAll("<TOTAL_SUCCESS_SUB>",
                        batchRechargeCmd.recharge_success + "")
                .replaceAll("<TOTAL_FAIL_SUB>",
                        batchRechargeCmd.recharge_failed + "")
                .replaceAll("<BALANCE>", "" + transactionRecord.balance_after )
                .replaceAll("<TRANS_ID>", "BR-" + transactionRecord.id);
        contentUssd = contentUssd
                .replaceAll("<DATE_TIME>",
                        getDateTimeFormated(transactionRecord.date_time))
                .replaceAll("<AMOUNT>",
                        "" + batchRechargeCmd.recharge_success_amount)
                .replaceAll("<TOTAL_SUCCESS_SUB>",  batchRechargeCmd.recharge_success + "")
                .replaceAll("<TOTAL_FAIL_SUB>", batchRechargeCmd.recharge_failed + "")
                .replaceAll("<BALANCE>", "" + transactionRecord.balance_after)
                .replaceAll("<TRANS_ID>", "BR-" + transactionRecord.id);
        sendSms(dealerRequest.msisdn, contentSms, contentUssd, SmsTypes.SMS_TYPE_BATCH_RECHARGE, transactionRecord.id);
        updateDealerRequest(dealerRequest);
        listRequestProcessing.remove(requestInfo.msisdn);
    }
	private void OnRefundGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp){
        logInfo(getSubInfoCmdResp.getRespString());
        RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
        if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_RECHARGE){
            OnRefundRechargeGetSubInfoResp(getSubInfoCmdResp);
        }else if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
            OnRefundRechargeGetSubInfoResp(getSubInfoCmdResp);
        }
	    
	}
	private void OnRefundRechargeGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp) {
        // TODO Auto-generated method stub
        RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
        AgentRequest agentRequest=requestInfo.agentRequest;
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        if(getSubInfoCmdResp.resultCode==PaymentGWResultCode.RC_GET_SUBS_INFO_SUCCESS){
            if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_PREPAID){
                transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_PREPAID;
                int subBalance=getSubInfoCmdResp.balance; 
                if(subBalance >= Config.MULTIPLIER ){
                    long chargeValue=agentRequest.refund_amount;
                    if( chargeValue > subBalance){
                        chargeValue= (subBalance/Config.MULTIPLIER)*Config.MULTIPLIER;
                    }
                    ChargingCmd chargingCmd = new ChargingCmd();
                    chargingCmd.msisdn=requestInfo.msisdn;
                    chargingCmd.recharge_msidn=requestInfo.old_transactionRecord.recharge_msidn;
                    
                    chargingCmd.transactionID=getPaymentGWTransactionId();
                    chargingCmd.spID=Config.charging_spID;
                    chargingCmd.serviceID=Config.charging_serviceID;
                    chargingCmd.chargeValue=chargeValue;
                    ChargingSession chargingSession = new ChargingSession(chargingCmd, queueChargingCmdResp, logger);
                    chargingSession.start();
                }else{
                    transactionRecord.balance_after =   transactionRecord.balance_before;
                    transactionRecord.result_description = "Balance is not enough.";
                    transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
                    transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
                    insertTransactionRecord(transactionRecord);
                    
                    agentRequest.status = AgentRequest.STATUS_FAILED;
                    agentRequest.result_code = AgentRequest.RC_REFUND_RECHARGE_SUBSCRIBER_BALANCE_NOT_ENOUGH;
                    updateAgentRequest(agentRequest);
                    listRequestProcessing.remove(requestInfo.msisdn);
                    logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Balance is not enough.");
                    return;
                }
            }
            else if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_POSTPAID){
                long refundAmount=agentRequest.refund_amount;
                agentRequest.balance_add_amount = refundAmount;
                agentRequest.dealer_id = requestInfo.old_transactionRecord.dealer_id;
                updateDealer(agentRequest);
                
                transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_POSTPAID;

                transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
                transactionRecord.balance_after = transactionRecord.balance_before+ refundAmount ;
                transactionRecord.balance_changed_amount=refundAmount;
                transactionRecord.recharge_value=-1*(int)refundAmount;
                transactionRecord.recharge_msidn= requestInfo.old_transactionRecord.recharge_msidn;
                
                transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
                transactionRecord.result_description = "Refund recharge postpaid subscriber success";
                insertTransactionRecord(transactionRecord);
                
                agentRequest.status = AgentRequest.STATUS_SUCCESS;
                agentRequest.result_code = AgentRequest.RC_REFUND_RECHARGE_SUCCESS;
                updateAgentRequest(agentRequest);
                listRequestProcessing.remove(requestInfo.msisdn);
                logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +" success.");
                
                String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_RECHARGE_DEALER_NOTIFY")
                        .replaceAll("<AMOUNT>", ""+refundAmount)
                        .replaceAll("<RECEIVER_NUMBER>", requestInfo.old_transactionRecord.recharge_msidn)
                        .replaceAll("<BALANCE>", ""+  transactionRecord.balance_after)
                        .replaceAll("<TRANS_ID>", ""+requestInfo.old_transactionRecord.id);
                String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_RECHARGE_DEALER_NOTIFY")
                        .replaceAll("<AMOUNT>", ""+refundAmount)
                        .replaceAll("<TRANS_ID>", ""+requestInfo.old_transactionRecord.id)
                        .replaceAll("<RECEIVER_NUMBER>", requestInfo.old_transactionRecord.recharge_msidn);
                sendSms(requestInfo.old_transactionRecord.dealer_msisdn, content, ussdContent, SmsTypes.SMS_TYPE_REFUND_RECHARGE, transactionRecord.id);
                
                if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
                    BatchRechargeElement batchRechargeElement=requestInfo.batchRechargeElement;
                    batchRechargeElement.refund_status=BatchRechargeElement.STATUS_SUCCESS;
                    batchRechargeElement.refund_result_code = 0;
                    batchRechargeElement.refund_result_string = "Refund for postPaid subscriber success.";
                    updateBatchRechargeElement(batchRechargeElement); // @NOTE
                }
                try {
                    requestInfo.old_transactionRecord.refund_status=TransactionRecord.TRANS_REFUNDED_STATUS;
                    connection.updateTransactionRecord(requestInfo.old_transactionRecord);
                }catch (SQLException e) {
                    isConnected = false;
                    logError(MySQLConnection.getSQLExceptionString(e));
                } catch (Exception e) {
                    e.printStackTrace();
                    logError(e.getMessage());
                }
            }
        }
        else{
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Get SubInfo Failed";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_RECHARGE_GET_SUBSCRIBER_INFO_FAILED;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Get SubInfo Failed");
            return;
        }
    }

    @SuppressWarnings("unused")
	private void onRefundBatchRechargeDone(RequestInfo requestInfo) {
        AgentRequest agentRequest=requestInfo.agentRequest;
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        BatchRechargeCmd batchRechargeCmd=(BatchRechargeCmd)requestInfo.dealerRequest.requestCmd;
        if( batchRechargeCmd.recharge_success<=0){
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.result_description = "Refund batch recharge fail.";
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_BATCH_RECHARGE_FAILED;
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; Refund batch recharge fail. "+batchRechargeCmd.getRespString());
        }else{
            long refundAmount=batchRechargeCmd.recharge_success_amount;
            agentRequest.balance_add_amount = refundAmount;
            agentRequest.dealer_id = requestInfo.old_transactionRecord.dealer_id;
            updateDealer(agentRequest);
            
            transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
            transactionRecord.result_description = "Refund batch recharge success.";
            agentRequest.status = AgentRequest.STATUS_SUCCESS;
            agentRequest.result_code = AgentRequest.RC_REFUND_BATCH_RECHARGE_SUCCESS;
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; Refund batch recharge success. "+batchRechargeCmd.getRespString());
            String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_BATCH_RECHARGE_DEALER_NOTIFY")
                    .replaceAll("<AMOUNT>", ""+refundAmount)
                    .replaceAll("<RECEIVER_NUMBER>", requestInfo.old_transactionRecord.recharge_msidn)
                    .replaceAll("<BALANCE>", ""+  transactionRecord.balance_after)
                    .replaceAll("<TRANS_ID>", ""+transactionRecord.id);
            String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_RECHARGE_DEALER_NOTIFY")
                    .replaceAll("<AMOUNT>", ""+refundAmount)
                    .replaceAll("<TRANS_ID>", ""+transactionRecord.id)
                    .replaceAll("<RECEIVER_NUMBER>", requestInfo.old_transactionRecord.recharge_msidn);
            sendSms(requestInfo.old_transactionRecord.dealer_msisdn, content, ussdContent, SmsTypes.SMS_TYPE_REFUND_RECHARGE, transactionRecord.id);
            try {
                requestInfo.old_transactionRecord.refund_status=TransactionRecord.TRANS_REFUNDED_STATUS;
                connection.updateTransactionRecord(requestInfo.old_transactionRecord);
            }catch (SQLException e) {
                isConnected = false;
                logError(MySQLConnection.getSQLExceptionString(e));
            } catch (Exception e) {
                e.printStackTrace();
                logError(e.getMessage());
            }
        }
        transactionRecord.balance_changed_amount=batchRechargeCmd.recharge_success_amount;
        transactionRecord.balance_after =  transactionRecord.balance_before+batchRechargeCmd.recharge_success_amount;
        transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
        insertTransactionRecord(transactionRecord);       
        updateAgentRequest(agentRequest);
        listRequestProcessing.remove(requestInfo.msisdn);
    }
	private void OnChargingCmdResp(ChargingCmd chargingCmdResp){
        logInfo("Charging resp:"+chargingCmdResp.toString());
        RequestInfo requestInfo = listRequestProcessing.get(chargingCmdResp.msisdn);
        if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_RECHARGE){
            OnRefundRechargeChargingCmdResp(chargingCmdResp);
        }else if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
            OnRefundRechargeChargingCmdResp(chargingCmdResp);
        }
        
	}
    private void OnRefundRechargeChargingCmdResp(ChargingCmd chargingCmdResp) {
        RequestInfo requestInfo = listRequestProcessing.get(chargingCmdResp.msisdn);
        AgentRequest agentRequest=requestInfo.agentRequest;
        TransactionRecord transactionRecord = requestInfo.transactionRecord;
        if (chargingCmdResp.resultCode == ChargingCmd.RESULT_OK) {
            agentRequest.balance_add_amount = chargingCmdResp.chargeValue;
            agentRequest.dealer_id = requestInfo.old_transactionRecord.dealer_id;
            updateDealer(agentRequest);

            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            transactionRecord.balance_after = transactionRecord.balance_before+ chargingCmdResp.chargeValue ;
            transactionRecord.balance_changed_amount=chargingCmdResp.chargeValue;
            transactionRecord.recharge_value=-1*(int)chargingCmdResp.chargeValue;
            transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
            transactionRecord.result_description = "Refund recharge prepaid subscriber success";
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_SUCCESS;
            agentRequest.result_code = AgentRequest.RC_REFUND_RECHARGE_SUCCESS;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +" success.");

            String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_RECHARGE_DEALER_NOTIFY")
                    .replaceAll("<AMOUNT>", ""+chargingCmdResp.chargeValue)
                    .replaceAll("<RECEIVER_NUMBER>", requestInfo.old_transactionRecord.recharge_msidn)
                    .replaceAll("<BALANCE>", ""+  transactionRecord.balance_after)
                    .replaceAll("<TRANS_ID>", "RS-"+requestInfo.old_transactionRecord.id);
            String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_RECHARGE_DEALER_NOTIFY")
                    .replaceAll("<AMOUNT>", ""+chargingCmdResp.chargeValue)
                    .replaceAll("<TRANS_ID>", "RS-"+requestInfo.old_transactionRecord.id)
                    .replaceAll("<RECEIVER_NUMBER>", requestInfo.old_transactionRecord.recharge_msidn);
            sendSms(requestInfo.old_transactionRecord.dealer_msisdn, content, ussdContent, SmsTypes.SMS_TYPE_REFUND_RECHARGE, transactionRecord.id);
         
            String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REFUND_RECHARGE_SUBSCRIBER_NOTIFY")
                    .replaceAll("<AMOUNT>", ""+chargingCmdResp.chargeValue)
                    .replaceAll("<DEALER_NUMBER>", ""+ requestInfo.old_transactionRecord.dealer_msisdn)
                    .replaceAll("<TRANS_ID>", "RS-"+requestInfo.old_transactionRecord.id);
            String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REFUND_RECHARGE_SUBSCRIBER_NOTIFY")
                    .replaceAll("<AMOUNT>", ""+chargingCmdResp.chargeValue)
                    .replaceAll("<DEALER_NUMBER>", ""+ requestInfo.old_transactionRecord.dealer_msisdn);
            String refundSub=chargingCmdResp.recharge_msidn;
            if( !refundSub.startsWith("856")){
                refundSub="856"+refundSub;
            }
            sendSms( refundSub, content1, ussdContent1, SmsTypes.SMS_TYPE_REFUND_RECHARGE, transactionRecord.id);
            if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
                BatchRechargeElement batchRechargeElement=requestInfo.batchRechargeElement;
                batchRechargeElement.refund_status=BatchRechargeElement.STATUS_SUCCESS;
                batchRechargeElement.refund_result_code = chargingCmdResp.resultCode;
                batchRechargeElement.refund_result_string = chargingCmdResp.resultString;
                updateBatchRechargeElement(batchRechargeElement); // @NOTE
            }
            try {
                requestInfo.old_transactionRecord.refund_status=TransactionRecord.TRANS_REFUNDED_STATUS;
                connection.updateTransactionRecord(requestInfo.old_transactionRecord);
                connection.insertRefundCDRRecord(chargingCmdResp.recharge_msidn, chargingCmdResp.chargeValue, chargingCmdResp.resultCode, chargingCmdResp.resultString, 2, chargingCmdResp.transactionID, Config.charging_spID, Config.charging_serviceID, transactionRecord.id);
            }catch (SQLException e) {
                isConnected = false;
                logError(MySQLConnection.getSQLExceptionString(e));
            } catch (Exception e) {
                e.printStackTrace();
                logError(e.getMessage());
            }
        }else{
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = chargingCmdResp.resultCode+"|"+chargingCmdResp.resultString;
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_code = AgentRequest.RC_REFUND_RECHARGE_CHARGING_FAILED;
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refund transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: charging failed");
            
            try {
                connection.insertRefundCDRRecord(chargingCmdResp.recharge_msidn, chargingCmdResp.chargeValue, chargingCmdResp.resultCode, chargingCmdResp.resultString, 3, chargingCmdResp.transactionID, Config.charging_spID, Config.charging_serviceID, transactionRecord.id);
            }catch (SQLException e) {
                isConnected = false;
                logError(MySQLConnection.getSQLExceptionString(e));
            } catch (Exception e) {
                e.printStackTrace();
                logError(e.getMessage());
            }
            
            return;
        }
    }
   
	private void updateBatchRechargeElement(BatchRechargeElement batchRechargeElement) {
		// TODO Auto-generated method stub
	    logInfo("Update for "+batchRechargeElement.toString());
	    GlobalVars.updateBatchRechargeElementProcess.queueBatchRechargeElement.enqueue(batchRechargeElement);
	}

	private void insertRechargeCdrRecord(RechargeCdrRecord rechargeCdrRecord) {
		// TODO Auto-generated method stub
		GlobalVars.insertRechargeCdrRecordProcess.queueInsertRechargeCdrRecordProcess.enqueue(rechargeCdrRecord);
	}

	private void OnDealerRequest(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		logInfo(dealerRequest.toString());
		dealerRequest.analizeContent();
		if(listRequestProcessing.containsKey(dealerRequest.msisdn)){
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_PREVIOUS_REQ_INPROGRESS");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_PREVIOUS_REQ_INPROGRESS");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_REJECT, 0);
			dealerRequest.result = "CONTENT_PREVIOUS_REQ_INPROGRESS";
			updateDealerRequest(dealerRequest);
			return;
		}
		
		RequestInfo requestInfo = new RequestInfo();
		requestInfo.msisdn = dealerRequest.msisdn;
		requestInfo.dealerRequest = dealerRequest;
		listRequestProcessing.put(requestInfo.msisdn, requestInfo);
		
		switch(dealerRequest.cmd_type){
		case DealerRequest.CMD_TYPE_QUERY_BALANCE:
			OnQueryBalance(dealerRequest);
			break;
		case DealerRequest.CMD_TYPE_CHANGE_PIN:
			OnChangePIN(dealerRequest);
			break;
		case DealerRequest.CMD_TYPE_RECHARGE:
			OnRecharge(dealerRequest);
			break;
		case DealerRequest.CMD_TYPE_MOVE_STOCK:
			OnMoveStock(dealerRequest);
			break;
		case DealerRequest.CMD_TYPE_BATCH_RECHARGE:
			OnBatchRecharge(dealerRequest);
			break;
		case DealerRequest.CMD_TYPE_WRONG_SYNTAX:
			OnReqWrongSyntax(dealerRequest);
			break;
		}
	}

	

	private void OnBatchRecharge(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		BatchRechargeCmd batchRechargeCmd = (BatchRechargeCmd)dealerRequest.requestCmd;
		logInfo(batchRechargeCmd.getReqString());
		if(!isDealer(dealerRequest, SmsTypes.SMS_TYPE_BATCH_RECHARGE))
			return;
		
		DealerInfo dealerInfo = batchRechargeCmd.dealerInfo;
		
		try {
			batchRechargeCmd.batchRechargeElements = connection.getBatchRechargeElementList(batchRechargeCmd.batch_recharge_id);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			batchRechargeCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
			batchRechargeCmd.resultString = MySQLConnection.getSQLExceptionString(e);
			logError(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return;
		}
		if(batchRechargeCmd.batchRechargeElements.isEmpty()){
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Batch recharge list not found";
			logError(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BATCH_RECHARGE_LIST_NOT_FOUND");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BATCH_RECHARGE_LIST_NOT_FOUND");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return;
		}
		
		int checkInfoStatus = batchRechargeCmd.checkInfo(dealerInfo);
		if(checkInfoStatus == BatchRechargeCmd.CHECK_STATUS_DEALER_INFO_NOT_MATCHED){
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Dealer info does not matched";
			logError(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DEALER_INFO_DOES_NOT_MATCHED");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DEALER_INFO_DOES_NOT_MATCHED");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_DEALER_INFO_DOES_NOT_MATCHED";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return;
		}
		else if(checkInfoStatus == BatchRechargeCmd.CHECK_STATUS_BATCH_RECHARGE_LIST_NOT_FOUND){
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Recharge list does not found";
			logError(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BATCH_RECHARGE_LIST_NOT_FOUND");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BATCH_RECHARGE_LIST_NOT_FOUND");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_BATCH_RECHARGE_LIST_NOT_FOUND";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return;
		}
		else if(checkInfoStatus == BatchRechargeCmd.CHECK_STATUS_INVALID_RECEIVER){
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Invalid receiver number";
			logError(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PHONE");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PHONE");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_WRONG_PHONE";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return;
		}
		else if(checkInfoStatus == BatchRechargeCmd.CHECK_STATUS_INVALID_AMOUNT){
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Invalid recharge amount";
			logError(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_AMOUNT");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_AMOUNT");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_WRONG_AMOUNT";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return;
		}
		else if(checkInfoStatus == BatchRechargeCmd.CHECK_STATUS_DOES_NOT_ENOUGH_BALANCE){
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Account has not enough balance";
			
			logInfo(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BALANCE_NOT_ENOUGH")
					.replaceAll("<BALANCE>", ""+dealerInfo.balance);
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BALANCE_NOT_ENOUGH").replaceAll("<BALANCE>", ""+dealerInfo.balance);
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_BALANCE_NOT_ENOUGH";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
		}
		else{

			RequestInfo requestInfo = listRequestProcessing.get(batchRechargeCmd.msisdn);
			requestInfo.transactionRecord = createTransactionRecord();
			TransactionRecord transactionRecord = requestInfo.transactionRecord;
			transactionRecord.type = TransactionRecord.TRANS_TYPE_BATCH_RECHARGE;
			transactionRecord.dealer_msisdn = dealerInfo.msisdn;
			transactionRecord.dealer_id = dealerInfo.id;
			transactionRecord.balance_before = dealerInfo.balance;
			transactionRecord.balance_changed_amount = 0;
			transactionRecord.recharge_msidn = "";
			transactionRecord.recharge_value = 0;
			transactionRecord.transaction_amount_req=-1*batchRechargeCmd.batch_recharge_total_amount;
			transactionRecord.batch_recharge_id=batchRechargeCmd.batch_recharge_id;
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
			transactionRecord.result_description = "Batch recharge success";
			
			batchRechargeCmd.resultCode = BatchRechargeCmd.R_TELCO_ERROR;
			batchRechargeCmd.resultString = "Batch recharge failed";
			
	        batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
	         
	        batchRechargeGetSubInfoForElement(requestInfo);
		}
	}
    private void batchRechargeGetSubInfoForElement(RequestInfo requestInfo) {
        BatchRechargeCmd batchRechargeCmd=(BatchRechargeCmd)requestInfo.dealerRequest.requestCmd;

        GetSubInfoCmd getSubInfoCmd = new GetSubInfoCmd();
        getSubInfoCmd.msisdn = batchRechargeCmd.currentBatchRechargeElement.dealer_msisdn;
        getSubInfoCmd.transactionId = getPaymentGWTransactionId();
        getSubInfoCmd.reqDate = new Date(System.currentTimeMillis());
        getSubInfoCmd.rechargeMsisdn = batchRechargeCmd.currentBatchRechargeElement.recharge_msisdn;
        getSubInfoCmd.token = token;
        getSubInfoCmd.queueResp = queuePaymentGWResp;
        logInfo(getSubInfoCmd.getReqString());
        GlobalVars.paymentGWInterface.queueUserRequest.enqueue(getSubInfoCmd);
    }
	private void OnRecharge(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		RechargeCmd rechargeCmd = (RechargeCmd) dealerRequest.requestCmd;
		String rechargeMsisdn = "";
		if(rechargeCmd.rechargeMsisdn.startsWith("0202") && rechargeCmd.rechargeMsisdn.length()==11){
			rechargeMsisdn = rechargeCmd.rechargeMsisdn.replaceFirst("0", "");
		}
		else if(rechargeCmd.rechargeMsisdn.startsWith("202") && rechargeCmd.rechargeMsisdn.length()==10){
			rechargeMsisdn = rechargeCmd.rechargeMsisdn;
		}
		else if(rechargeCmd.rechargeMsisdn.startsWith("0302") && rechargeCmd.rechargeMsisdn.length()==10){
			rechargeMsisdn = rechargeCmd.rechargeMsisdn.replaceFirst("0", "");
		}
		else if(rechargeCmd.rechargeMsisdn.startsWith("302") && rechargeCmd.rechargeMsisdn.length()==9){
			rechargeMsisdn = rechargeCmd.rechargeMsisdn;
		}
		if(!rechargeMsisdn.equals(""))
			rechargeCmd.rechargeMsisdn = rechargeMsisdn;
		logInfo(rechargeCmd.getReqString());
		if(!isDealer(dealerRequest, SmsTypes.SMS_TYPE_RECHARGE))
			return;
		
		DealerInfo dealerInfo = rechargeCmd.dealerInfo;
		RequestInfo requestInfo = listRequestProcessing.get(dealerRequest.msisdn);
		requestInfo.transactionRecord = createTransactionRecord();
		TransactionRecord transactionRecord = requestInfo.transactionRecord;
		transactionRecord.type = TransactionRecord.TRANS_TYPE_RECHARGE;
		transactionRecord.dealer_msisdn = dealerInfo.msisdn;
		transactionRecord.dealer_id = dealerInfo.id;
		transactionRecord.balance_before = dealerInfo.balance;
		transactionRecord.transaction_amount_req = -1*rechargeCmd.amount;
		transactionRecord.balance_changed_amount = 0;
		transactionRecord.recharge_msidn = rechargeCmd.rechargeMsisdn;
		transactionRecord.recharge_value = rechargeCmd.amount;
		dealerRequest.dealer_id = dealerInfo.id;
		dealerRequest.transaction_id = transactionRecord.id;
		
		if(rechargeMsisdn!=null){
			DelayRecharge checkDelayRecharge = listDelayRecharges.get(dealerRequest.msisdn+"_"+rechargeMsisdn);
			if(checkDelayRecharge!=null&&checkDelayRecharge.amount == rechargeCmd.amount){
				long checkCurrentTime = System.currentTimeMillis();
				if(checkCurrentTime-checkDelayRecharge.timestamp<Config.consecutiveTransactionDelayTime*60000){
					rechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
					rechargeCmd.resultString = "Rejected cause by consecutive request";
					logInfo(rechargeCmd.getRespString());
					transactionRecord.balance_after = dealerInfo.balance;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = rechargeCmd.resultString;
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECHARGE_CONSECUTIVE").replaceAll("<DELAY_TIME>", ""+Config.consecutiveTransactionDelayTime);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECHARGE_CONSECUTIVE").replaceAll("<DELAY_TIME>", ""+Config.consecutiveTransactionDelayTime);
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
					dealerRequest.result = "CONTENT_RECHARGE_CONSECUTIVE";
					dealerRequest.dealer_id = dealerInfo.id;
					dealerRequest.transaction_id = transactionRecord.id;
					insertTransactionRecord(transactionRecord);
					updateDealerRequest(dealerRequest);
					listRequestProcessing.remove(dealerRequest.msisdn);
					return;
				}
				else{
					listDelayMoveStocks.remove(dealerRequest.msisdn+"_"+rechargeMsisdn);
				}
			}
		}
		
		boolean isValidPIN = dealerInfo.pin_code.equals(rechargeCmd.pinCode);
		boolean isValidAmount = (rechargeCmd.amount>=5000&&rechargeCmd.amount%5000==0)?true:false;
		
		if(rechargeMsisdn.equals("") || !isValidPIN || !isValidAmount||rechargeCmd.amount>dealerInfo.balance){
			rechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			

			String content;
			String ussdContent;

			if(rechargeMsisdn.equals("") && !isValidPIN && !isValidAmount){				
				rechargeCmd.resultString = "Wrong PIN, Phone number & Amount";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_PHONE_AMOUNT");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_PHONE_AMOUNT");
				dealerRequest.result = "CONTENT_WRONG_PIN_PHONE_AMOUNT";
			}
			else if(rechargeMsisdn.equals("") && !isValidAmount){
				rechargeCmd.resultString = "Wrong Phone number & Amount";				
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PHONE_AMOUNT");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PHONE_AMOUNT");
				dealerRequest.result = "CONTENT_WRONG_PHONE_AMOUNT";				
			}
			else if(!isValidPIN && !isValidAmount){
				rechargeCmd.resultString = "Wrong PIN & Amount";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_AMOUNT");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_AMOUNT");
				dealerRequest.result = "CONTENT_WRONG_PIN_AMOUNT";
			}
			else if(rechargeMsisdn.equals("") && !isValidPIN){				
				rechargeCmd.resultString = "Wrong PIN & Phone number";			
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_PHONE");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_PHONE");
				dealerRequest.result = "CONTENT_WRONG_PIN_PHONE";
			}
			else if(!isValidAmount){
				rechargeCmd.resultString = "Wrong Amount";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_AMOUNT");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_AMOUNT");
				dealerRequest.result = "CONTENT_WRONG_AMOUNT";

			}
			else if(rechargeMsisdn.equals("")){
				rechargeCmd.resultString = "Wrong Phone number";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PHONE");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PHONE");
				dealerRequest.result = "CONTENT_WRONG_PHONE";
			}
			else if(!isValidPIN){
				rechargeCmd.resultString = "Wrong PIN";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN");
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN");
				dealerRequest.result = "CONTENT_WRONG_PIN";
			}
			else{			
				rechargeCmd.resultString = "Account has not enough balance";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BALANCE_NOT_ENOUGH")
						.replaceAll("<BALANCE>", ""+dealerInfo.balance);
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BALANCE_NOT_ENOUGH").replaceAll("<BALANCE>", ""+dealerInfo.balance);
				dealerRequest.result = "CONTENT_BALANCE_NOT_ENOUGH";
			}
			transactionRecord.result_description = rechargeCmd.resultString;
			logInfo(rechargeCmd.getRespString());
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			insertTransactionRecord(transactionRecord);
		}
		else{	
			GetSubInfoCmd getSubInfoCmd = new GetSubInfoCmd();
			getSubInfoCmd.msisdn = rechargeCmd.msisdn;
			getSubInfoCmd.transactionId = getPaymentGWTransactionId();
			getSubInfoCmd.reqDate = new Date(System.currentTimeMillis());
			getSubInfoCmd.rechargeMsisdn = rechargeCmd.rechargeMsisdn;
			getSubInfoCmd.token = token;
			getSubInfoCmd.queueResp = queuePaymentGWResp;
			logInfo(getSubInfoCmd.getReqString());
			GlobalVars.paymentGWInterface.queueUserRequest.enqueue(getSubInfoCmd);
		}

	}

	private void OnMoveStock(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		MoveStockCmd moveStockCmd = (MoveStockCmd) dealerRequest.requestCmd;
		logInfo(moveStockCmd.getReqString());
		if(!isDealer(dealerRequest, SmsTypes.SMS_TYPE_MOVE_STOCK))
			return;
		DealerInfo dealerInfo = moveStockCmd.dealerInfo;
		DealerInfo receiverInfo = null;

		TransactionRecord transactionRecord = createTransactionRecord();
		transactionRecord.type = TransactionRecord.TRANS_TYPE_MOVE_STOCK;
		transactionRecord.dealer_msisdn = dealerInfo.msisdn;
		transactionRecord.dealer_id = dealerInfo.id;
		transactionRecord.transaction_amount_req = -1*moveStockCmd.amount;
		transactionRecord.balance_before = dealerInfo.balance;
		transactionRecord.balance_changed_amount = 0;
		transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
		
		String receiverMsisdn = "";
		if(moveStockCmd.receiverMsisdn.startsWith("0202") && moveStockCmd.receiverMsisdn.length()==11){
			receiverMsisdn = moveStockCmd.receiverMsisdn.replaceFirst("0", "856");
		}
		else if(moveStockCmd.receiverMsisdn.startsWith("202") && moveStockCmd.receiverMsisdn.length()==10){
			receiverMsisdn = "856"+moveStockCmd.receiverMsisdn;
		}
		else if(moveStockCmd.receiverMsisdn.startsWith("0302") && moveStockCmd.receiverMsisdn.length()==10){
			receiverMsisdn = moveStockCmd.receiverMsisdn.replaceFirst("0", "856");
		}
		else if(moveStockCmd.receiverMsisdn.startsWith("302") && moveStockCmd.receiverMsisdn.length()==9){
			receiverMsisdn = "856"+moveStockCmd.receiverMsisdn;
		}
		
		if(receiverMsisdn!=null){
			DelayMoveStock checkDelayMoveStock = listDelayMoveStocks.get(moveStockCmd.msisdn+"_"+receiverMsisdn);
			if(checkDelayMoveStock!=null&&checkDelayMoveStock.amount == moveStockCmd.amount){
				long checkCurrentTime = System.currentTimeMillis();
				if(checkCurrentTime-checkDelayMoveStock.timestamp<Config.consecutiveTransactionDelayTime*60000){
					moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
					moveStockCmd.resultString = "Rejected cause by consecutive request";
					logInfo(moveStockCmd.getRespString());
					transactionRecord.balance_after = dealerInfo.balance;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = moveStockCmd.resultString;
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_MOVE_STOCK_CONSECUTIVE").replaceAll("<DELAY_TIME>", ""+Config.consecutiveTransactionDelayTime);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_MOVE_STOCK_CONSECUTIVE").replaceAll("<DELAY_TIME>", ""+Config.consecutiveTransactionDelayTime);
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
					dealerRequest.result = "CONTENT_MOVE_STOCK_CONSECUTIVE";
					dealerRequest.dealer_id = dealerInfo.id;
					dealerRequest.transaction_id = transactionRecord.id;
					insertTransactionRecord(transactionRecord);
					updateDealerRequest(dealerRequest);
					listRequestProcessing.remove(dealerRequest.msisdn);
					return;
				}
				else{
					listDelayMoveStocks.remove(moveStockCmd.msisdn+"_"+moveStockCmd.receiverMsisdn);
				}
			}
		}
		
		boolean isValidPIN = dealerInfo.pin_code.equals(moveStockCmd.pinCode);
		boolean isValidAmount = (moveStockCmd.amount>=5000&&moveStockCmd.amount%5000==0)?true:false;
		if(receiverMsisdn.equals("") && !isValidPIN && !isValidAmount){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong PIN, Phone number & Amount";
			logInfo(moveStockCmd.getRespString());
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_PHONE_AMOUNT");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_PHONE_AMOUNT");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_PIN_PHONE_AMOUNT";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			insertTransactionRecord(transactionRecord);
		}
		else if(receiverMsisdn.equals("") && !isValidAmount){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong Phone number & Amount";
			logInfo(moveStockCmd.getRespString());
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PHONE_AMOUNT");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PHONE_AMOUNT");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_PHONE_AMOUNT";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			insertTransactionRecord(transactionRecord);
		}
		else if(!isValidPIN && !isValidAmount){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong PIN & Amount";
			logInfo(moveStockCmd.getRespString());
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_AMOUNT");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_AMOUNT");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_PIN_AMOUNT";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			insertTransactionRecord(transactionRecord);
		}
		else if(receiverMsisdn.equals("") && !isValidPIN){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong PIN & Phone number";
			logInfo(moveStockCmd.getRespString());
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_PHONE");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_PHONE");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_PIN_PHONE";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			insertTransactionRecord(transactionRecord);
		}
		else if(!isValidAmount){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong Amount";
			logInfo(moveStockCmd.getRespString());

			
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			insertTransactionRecord(transactionRecord);

			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_AMOUNT");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_AMOUNT");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_AMOUNT";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
		}
		else if(receiverMsisdn.equals("")){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong Phone number";
			logInfo(moveStockCmd.getRespString());
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PHONE");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PHONE");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_PHONE";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			insertTransactionRecord(transactionRecord);
		}
		else if(!isValidPIN){
			moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			moveStockCmd.resultString = "Wrong PIN";
			logInfo(moveStockCmd.getRespString());
			transactionRecord.balance_after = dealerInfo.balance;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
			transactionRecord.result_description = moveStockCmd.resultString;
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
			dealerRequest.result = "CONTENT_WRONG_PIN";
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			insertTransactionRecord(transactionRecord);
		}
		else{
			try {
				receiverInfo = connection.getDealerInfo(receiverMsisdn);
				moveStockCmd.receiverInfo = receiverInfo;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				isConnected = false;
				moveStockCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
				moveStockCmd.resultString = MySQLConnection.getSQLExceptionString(e);
				logError(moveStockCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
				sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, 0);
				dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
				dealerRequest.dealer_id = dealerInfo.id;
				updateDealerRequest(dealerRequest);
				listRequestProcessing.remove(dealerRequest.msisdn);
				return;
			}
			if(receiverInfo!=null&&dealerInfo.id==receiverInfo.parent_id){
				transactionRecord.partner_msisdn = receiverInfo.msisdn;
				transactionRecord.partner_id = receiverInfo.id;
				transactionRecord.partner_balance_before = receiverInfo.balance;
				if(moveStockCmd.amount>dealerInfo.balance){
					moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
					moveStockCmd.resultString = "Account has not enough balance";
					logInfo(moveStockCmd.getRespString());
					transactionRecord.balance_after = dealerInfo.balance;
					transactionRecord.partner_balance_after = receiverInfo.balance;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = moveStockCmd.resultString;
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BALANCE_NOT_ENOUGH").replaceAll("<BALANCE>", ""+dealerInfo.balance);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BALANCE_NOT_ENOUGH");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
					dealerRequest.result = "CONTENT_BALANCE_NOT_ENOUGH";
					dealerRequest.dealer_id = dealerInfo.id;
					dealerRequest.transaction_id = transactionRecord.id;
					insertTransactionRecord(transactionRecord);
				}
				else{
					try {
						connection.moveStock(moveStockCmd);
						if(moveStockCmd.db_return_code==0){
							DelayMoveStock delayMoveStock = new DelayMoveStock();
							delayMoveStock.amount = moveStockCmd.amount;
							listDelayMoveStocks.put(moveStockCmd.msisdn+"_"+receiverMsisdn, delayMoveStock);
							transactionRecord.balance_changed_amount = -1*moveStockCmd.amount;
							transactionRecord.balance_after = moveStockCmd.balanceAfter;
							transactionRecord.partner_balance_after = moveStockCmd.receiverBalanceAfter;
							transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
							transactionRecord.result_description = "Move Stock successfully";

							moveStockCmd.resultCode = RequestCmd.R_OK;
							moveStockCmd.resultString = "Move Stock successfully";
							logError(moveStockCmd.getRespString());
							String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_MOVE_STOCK_SUCCESS")
									.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
									.replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
									.replaceAll("<RECEIVER_NUMBER>", moveStockCmd.receiverMsisdn)
									.replaceAll("<BALANCE>", ""+moveStockCmd.balanceAfter)
									.replaceAll("<TRANS_ID>", "MS-"+transactionRecord.id);
							String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_MOVE_STOCK_SUCCESS")
									.replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
									.replaceAll("<RECEIVER_NUMBER>", moveStockCmd.receiverMsisdn);
							sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
							String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_MOVE_STOCK_SUCCESS_NOTIFY")
									.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
									.replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
									.replaceAll("<DEALER>", moveStockCmd.msisdn.replaceFirst("856", "0"));
							String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_MOVE_STOCK_SUCCESS_NOTIFY")
									.replaceAll("<AMOUNT>", ""+moveStockCmd.amount)
									.replaceAll("<DEALER>", moveStockCmd.msisdn.replaceFirst("856", "0"));
							sendSms(receiverMsisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
							dealerRequest.result = "CONTENT_MOVE_STOCK_SUCCESS";
							dealerRequest.dealer_id = dealerInfo.id;
							dealerRequest.transaction_id = transactionRecord.id;
							insertTransactionRecord(transactionRecord);
						}
						else{

							transactionRecord.balance_after = moveStockCmd.balanceAfter;
							transactionRecord.partner_balance_after = moveStockCmd.receiverBalanceAfter;
							transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
							transactionRecord.result_description = "Execute SQL function failed";
							moveStockCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
							moveStockCmd.resultString = "Execute SQL function failed";
							logError(moveStockCmd.getRespString());
							String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_MOVE_STOCK_FUNCTION_ERROR");
							String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_MOVE_STOCK_FUNCTION_ERROR");
							sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
							dealerRequest.result = "CONTENT_DB_MOVE_STOCK_FUNCTION_ERROR";
							dealerRequest.dealer_id = dealerInfo.id;
							dealerRequest.transaction_id = transactionRecord.id;
							insertTransactionRecord(transactionRecord);
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						isConnected = false;
						moveStockCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
						moveStockCmd.resultString = MySQLConnection.getSQLExceptionString(e);
						logError(moveStockCmd.getRespString());
						String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
						String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
						sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, 0);
						dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
						dealerRequest.dealer_id = dealerInfo.id;
						updateDealerRequest(dealerRequest);
						listRequestProcessing.remove(dealerRequest.msisdn);
						return;
					}
				}
			}
			else{
				moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
				moveStockCmd.resultString = "The receiver is not a sub-dealer";
				logInfo(moveStockCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_IS_NOT_SUB_DEALER");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_IS_NOT_SUB_DEALER");
				sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, 0);
				dealerRequest.result = "CONTENT_RECEIVER_IS_NOT_SUB_DEALER";
				dealerRequest.dealer_id = dealerInfo.id;
			}
		}
		updateDealerRequest(dealerRequest);
		listRequestProcessing.remove(dealerRequest.msisdn);
	}

	private void OnChangePIN(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		ChangePinCmd changePinCmd = (ChangePinCmd) dealerRequest.requestCmd;
		logInfo(changePinCmd.getReqString());
		if(!isDealer(dealerRequest, SmsTypes.SMS_TYPE_CHANGE_PIN))
			return;
		DealerInfo dealerInfo = changePinCmd.dealerInfo;

		if(!dealerInfo.pin_code.equals(changePinCmd.pinCode)){
			changePinCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			changePinCmd.resultString = "Wrong old PIN";
			logInfo(changePinCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_OLD_PIN");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_OLD_PIN");
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CHANGE_PIN, 0);
			dealerRequest.result = "CONTENT_WRONG_OLD_PIN";
			dealerRequest.dealer_id = dealerInfo.id;
		}
		else{
			if(changePinCmd.newPin.length()!=4||StringFunction.addrCompare(changePinCmd.newPin, "?000")){
				changePinCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
				changePinCmd.resultString = "Wrong PIN format";
				logInfo(changePinCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_PIN_FORMAT");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_PIN_FORMAT");
				sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CHANGE_PIN, 0);
				dealerRequest.result = "CONTENT_WRONG_PIN_FORMAT";
				dealerRequest.dealer_id = dealerInfo.id;
			}
			else{
				changePinCmd.dealerInfo = dealerInfo;
				try {
					connection.changePIN(changePinCmd);
					changePinCmd.resultCode = RequestCmd.R_OK;
					changePinCmd.resultString = "Change PIN success";
					logInfo(changePinCmd.getRespString());
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_CHANGE_PIN_SUCCESS")
							.replaceAll("<NEW_PIN>", changePinCmd.newPin);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_CHANGE_PIN_SUCCESS");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CHANGE_PIN, 0);
					dealerRequest.result = "CONTENT_CHANGE_PIN_SUCCESS";
					dealerRequest.dealer_id = dealerInfo.id;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					changePinCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
					changePinCmd.resultString = MySQLConnection.getSQLExceptionString(e);
					logError(changePinCmd.getRespString());
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CHANGE_PIN, 0);
					dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
				}
			}
		}
		updateDealerRequest(dealerRequest);
		listRequestProcessing.remove(dealerRequest.msisdn);
	}

	private void OnQueryBalance(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		QueryBalanceCmd queryBalanceCmd = (QueryBalanceCmd) dealerRequest.requestCmd;
		logInfo(queryBalanceCmd.getReqString());
		if(!isDealer(dealerRequest, SmsTypes.SMS_TYPE_QUERY_BALANCE))
			return;

		DealerInfo dealerInfo = queryBalanceCmd.dealerInfo;
		queryBalanceCmd.resultCode = RequestCmd.R_OK;
		queryBalanceCmd.dealerAccountBalance = dealerInfo.balance;
		logInfo(queryBalanceCmd.getRespString());
		String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_QUERY_BALANCE").replaceAll("<DEALER_BALANCE>", ""+queryBalanceCmd.dealerAccountBalance);
		String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_QUERY_BALANCE").replaceAll("<DEALER_BALANCE>", ""+queryBalanceCmd.dealerAccountBalance);
		sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_QUERY_BALANCE, 0);
		dealerRequest.result = "CONTENT_QUERY_BALANCE";
		dealerRequest.dealer_id = dealerInfo.id;
		updateDealerRequest(dealerRequest);
		listRequestProcessing.remove(dealerRequest.msisdn);
	}
	
	public void OnReqWrongSyntax(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_WRONG_SYNTAX");
		String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_WRONG_SYNTAX");
		sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_WRONG_SYNTAX, 0);
		dealerRequest.result = "CONTENT_WRONG_SYNTAX";
		updateDealerRequest(dealerRequest);
		listRequestProcessing.remove(dealerRequest.msisdn);
	}

	private void updateDealerRequest(DealerRequest dealerRequest) {
		// TODO Auto-generated method stub
		try {
			connection.updateDealerRequest(dealerRequest);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logError("Update "+dealerRequest.toString()+"; error:"+e.getMessage());
		}
	}

	public void getDealerRequests() {
		long curTime = System.currentTimeMillis();
		if (dealerRequests.isEmpty()) {
			if (nextTimeGetDealerRequests < curTime) {
				try {
					dealerRequests = connection.getDealerRequestList();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logError("GetDealerRequestList: error:" + e.getMessage());
					isConnected = false;
					dealerRequests = new Vector<DealerRequest>();
				}
				if (dealerRequests.size() == 0) {
					nextTimeGetDealerRequests = curTime + 5000;
				} else {
					nextTimeGetDealerRequests = curTime;
				}
			}
		}
	}
	
	public void getAgentRequests() {
		long curTime = System.currentTimeMillis();
		if (agentRequests.isEmpty()) {
			if (nextTimeGetAgentRequests < curTime) {
				try {
					agentRequests = connection.getAgentRequestList();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logError("GetAgentRequestList: error:" + MySQLConnection.getSQLExceptionString(e));
					isConnected = false;
					agentRequests = new Vector<AgentRequest>();
				}
				if (agentRequests.isEmpty()) {
					nextTimeGetAgentRequests = curTime + 5000;
				} else {
					nextTimeGetAgentRequests = curTime;
				}
			}
		}
	}
	
	private void sendSms(String msisdn, String content, String ussdContent, int sms_type, int transaction_id) {
		// TODO Auto-generated method stub
		MTRecord mtRecord = new MTRecord(msisdn, content, sms_type, transaction_id);
		GlobalVars.insertSmsMTReqProcess.queueInsertMTReq.enqueue(mtRecord);
		logInfo("SendSms: msisdn:" + msisdn + "; content:" + content);
		
		MTRecord ussdNotify = new MTRecord(msisdn, ussdContent, 0, transaction_id);
		GlobalVars.insertUssdNotifyReqProcess.queueInsertNotifyReq.enqueue(ussdNotify);
		logInfo("SendUSSD: msisdn:" + msisdn + "; content:" + content);
	}
	
	private void insertTransactionRecord(TransactionRecord transactionRecord) {
		// TODO Auto-generated method stub
		GlobalVars.insertTransactionRecordProcess.queueInsertTransactionRecord.enqueue(transactionRecord);
	}
	
	public TransactionRecord createTransactionRecord(){
		TransactionRecord transactionRecord = new TransactionRecord();
		while(true){
			try {
				transactionRecord.id = connection.getETopupTransactionId();
				transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
				return transactionRecord;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logError("Get ETopup TransactionId error: "+MySQLConnection.getSQLExceptionString(e));
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				Connect();
			}
		}
	}
	
	public static String getDateTimeFormated(Timestamp timestamp){
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		return sdf.format(new Date(timestamp.getTime()));
	}
	
	
	public boolean isDealer(DealerRequest dealerRequest, int smsType) {
		// TODO Auto-generated method stub
		DealerInfo dealerInfo = null;
		try {
			dealerInfo = connection.getDealerInfo(dealerRequest.msisdn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			dealerRequest.requestCmd.resultCode = RequestCmd.R_SYSTEM_ERROR;
			dealerRequest.requestCmd.resultString = MySQLConnection.getSQLExceptionString(e);
			logError(dealerRequest.requestCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_DB_CONNECTION_ERROR");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_DB_CONNECTION_ERROR");
			sendSms(dealerRequest.msisdn, content, ussdContent, smsType, 0);
			dealerRequest.result = "CONTENT_DB_CONNECTION_ERROR";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return false;
		}
		if(dealerInfo==null){
			dealerRequest.requestCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			dealerRequest.requestCmd.resultString = "Subscriber is not a dealer";
			logInfo(dealerRequest.requestCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_IS_NOT_DEALER");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_IS_NOT_DEALER");
			sendSms(dealerRequest.msisdn, content, ussdContent, smsType, 0);
			dealerRequest.result = "CONTENT_IS_NOT_DEALER";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
			return false;
		}
		else{
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.requestCmd.dealerInfo = dealerInfo;
			return true;
		}
	}
}
