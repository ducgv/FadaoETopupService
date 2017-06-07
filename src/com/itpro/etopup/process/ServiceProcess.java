/**
 * 
 */
package com.itpro.etopup.process;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Random;
import java.util.Vector;



import com.itpro.etopup.db.DbConnection;
import com.itpro.etopup.main.Config;
import com.itpro.etopup.main.GlobalVars;
import com.itpro.etopup.struct.AddBalanceRate;
import com.itpro.etopup.struct.AgentRequest;
import com.itpro.etopup.struct.ChargingCmd;
import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;
import com.itpro.etopup.struct.MTRecord;
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
	
	private Vector<DealerRequest> dealerRequests = new Vector<DealerRequest>();
	private Vector<AgentRequest> agentRequests = new Vector<AgentRequest>();
	private Hashtable<String, RequestInfo> listRequestProcessing = new Hashtable<String, RequestInfo>();
	
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
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logError("Load ServiceConfigs error:" + MySQLConnection.getSQLExceptionString(e));
			isConnected = false;
		}
		if(Config.addBalanceRates==null){
			try {
				Config.addBalanceRates = connection.getConfigAddBalanceRate();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logError("Load AddBalanceRate error:" + MySQLConnection.getSQLExceptionString(e));
				isConnected = false;
			}
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
		logInfo(agentRequest.toString());
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
		    onRefund(requestInfo);
		    break;
		default:
			break;
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
			agentRequest.result_description = MySQLConnection.getSQLExceptionString(e);
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(dealerInfo==null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_description = "CONTENT_NOT_IS_DEALER";
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			logInfo("AddBalance: msisdn:"+requestInfo.msisdn +"; error: Number not is Dealer");
		}
		else{
			if(!Config.addBalanceRates.isEmpty()){
				AddBalanceRate addBalanceRate = null;
				for(AddBalanceRate tmp:Config.addBalanceRates){
					logInfo(tmp.toString());
					if(requestInfo.agentRequest.cash_value<tmp.cashLevel){
						addBalanceRate = tmp;
						break;
					}
				}
				if(addBalanceRate == null){
					agentRequest.status = AgentRequest.STATUS_FAILED;
					agentRequest.result_description = "CONTENT_NOT_FOUND_ADD_BALANCE_RATE";
					logError(agentRequest.getRespString());
					updateAgentRequest(agentRequest);
					listRequestProcessing.remove(requestInfo.msisdn);
					return;
				}
				else{
					logInfo(addBalanceRate.toString());
					TransactionRecord transactionRecord = createTransactionRecord();
					agentRequest.balance_add_amount = new Double(agentRequest.cash_value*addBalanceRate.rate/100).longValue();
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
					transactionRecord.agent = agentRequest.agent_username;
					transactionRecord.agent_id = agentRequest.agent_id;
					transactionRecord.cash_value = agentRequest.cash_value;
					transactionRecord.invoice_code = agentRequest.invoice_code;
					transactionRecord.result_description = "Add ETopup balance successfully";
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
					insertTransactionRecord(transactionRecord);
					agentRequest.status = AgentRequest.STATUS_SUCCESS;
					agentRequest.dealer_id = dealerInfo.id;
					agentRequest.result_description = "CONTENT_ADD_BALANCE_SUCCESS";
					agentRequest.transaction_id = transactionRecord.id;
					updateAgentRequest(agentRequest);
					logInfo(agentRequest.getRespString());
					
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_ADD_BALANCE_SUCCESS")
							.replaceAll("<AMOUNT>", ""+agentRequest.balance_add_amount)
							.replaceAll("<TRANS_ID>", ""+transactionRecord.id)
							.replaceAll("<BALANCE>", ""+dealerInfo.balance);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_ADD_BALANCE_SUCCESS")
							.replaceAll("<AMOUNT>", ""+agentRequest.balance_add_amount)
							.replaceAll("<TRANS_ID>", ""+transactionRecord.id)
							.replaceAll("<BALANCE>", ""+dealerInfo.balance);
					sendSms(requestInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_ADD_BALANCE, transactionRecord.id);
					
					listRequestProcessing.remove(requestInfo.msisdn);
				}
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
		AgentRequest agentRequest = requestInfo.agentRequest;
		try {
			dealerInfo = connection.getDealerInfo(requestInfo.msisdn);
			requestInfo.dealerInfo = dealerInfo;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			isConnected = false;
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_description = MySQLConnection.getSQLExceptionString(e);
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			return;
		}
		if(dealerInfo!=null){
			agentRequest.status = AgentRequest.STATUS_FAILED;
			agentRequest.result_description = "CONTENT_NUMBER_IS_USING_SERVICE";
			logError(agentRequest.getRespString());
			updateAgentRequest(agentRequest);
			listRequestProcessing.remove(requestInfo.msisdn);
			logInfo("Create Dealer: msisdn:"+requestInfo.msisdn +"; error: Number is using service");
		}
		else{
			if(!Config.addBalanceRates.isEmpty()){
				AddBalanceRate addBalanceRate = null;
				for(AddBalanceRate tmp:Config.addBalanceRates){
					logInfo(tmp.toString());
					if(requestInfo.agentRequest.cash_value<tmp.cashLevel){
						addBalanceRate = tmp;
						break;
					}
				}
				if(addBalanceRate == null){
					agentRequest.status = AgentRequest.STATUS_FAILED;
					agentRequest.result_description = "CONTENT_NOT_FOUND_ADD_BALANCE_RATE";
					logError(agentRequest.getRespString());
					updateAgentRequest(agentRequest);
					listRequestProcessing.remove(requestInfo.msisdn);
					return;
				}
				else{
					logInfo(addBalanceRate.toString());
					agentRequest.balance_add_amount = new Double(agentRequest.cash_value*addBalanceRate.rate/100).longValue();
					dealerInfo = new DealerInfo();
					dealerInfo.active = 1;
					dealerInfo.address = agentRequest.dealer_address;
					dealerInfo.agent_approved = agentRequest.agent_username;
					dealerInfo.agent_approved_id = agentRequest.agent_id;
					dealerInfo.balance = agentRequest.balance_add_amount;
					dealerInfo.birth_date = agentRequest.dealer_birthdate;
					dealerInfo.id_card_number = agentRequest.dealer_id_card_number;
					dealerInfo.msisdn = agentRequest.dealer_msisdn;
					dealerInfo.name = agentRequest.dealer_name;
					dealerInfo.pin_code = genRandPinCode();
					dealerInfo.province = agentRequest.dealer_province;
					dealerInfo.register_date = new Timestamp(System.currentTimeMillis());
					insertDealer(dealerInfo);
					
					TransactionRecord transactionRecord = createTransactionRecord();
					transactionRecord.dealer_msisdn = requestInfo.msisdn;
					transactionRecord.dealer_id = dealerInfo.id;
					transactionRecord.balance_before = 0;
					transactionRecord.balance_after = dealerInfo.balance;
					transactionRecord.type = TransactionRecord.TRANS_TYPE_CREATE_ACCOUNT;
					transactionRecord.balance_changed_amount = agentRequest.balance_add_amount;
					transactionRecord.agent = agentRequest.agent_username;
					transactionRecord.agent_id = agentRequest.agent_id;
					transactionRecord.cash_value = agentRequest.cash_value;
					transactionRecord.invoice_code = agentRequest.invoice_code;
					transactionRecord.result_description = "Register ETopup service successfully";
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					insertTransactionRecord(transactionRecord);
					agentRequest.status = AgentRequest.STATUS_SUCCESS;
					agentRequest.dealer_id = dealerInfo.id;
					agentRequest.result_description = "CONTENT_REGISTER_DEALER_SUCCESS";
					agentRequest.transaction_id = transactionRecord.id;
					updateAgentRequest(agentRequest);
					logInfo(agentRequest.getRespString());
					
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_REGISTER_DEALER_SUCCESS")
							.replaceAll("<PIN>", dealerInfo.pin_code)
							.replaceAll("<BALANCE>", ""+dealerInfo.balance);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_REGISTER_DEALER_SUCCESS")
							.replaceAll("<PIN>", dealerInfo.pin_code)
							.replaceAll("<BALANCE>", ""+dealerInfo.balance);
					sendSms(requestInfo.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_CREATE_ACCOUNT, transactionRecord.id);
					
					listRequestProcessing.remove(requestInfo.msisdn);
				}
			}
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
            agentRequest.result_description = MySQLConnection.getSQLExceptionString(e);
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            return;
        }
        if(old_transactionRecord==null){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_description = "CONTENT_TRANSACTION_NOT_FOUND";
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: transaction not found.");
            return;
        }
        if( old_transactionRecord.status !=TransactionRecord.TRANS_STATUS_SUCCESS){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_description = "CONTENT_TRANSACTION_NOT_SUCCESS";
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: refund for transaction not success.");
            return;
        }
        if( old_transactionRecord.refund_status==TransactionRecord.TRANS_REFUNDED_STATUS){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_description = "CONTENT_TRANSACTION_REFUNDED";
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: refund for transaction refuned.");
            return;
        }
        if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_RECHARGE){
            // get subinfo
            // charging
            //updatestatys
            onRefundRecharge(requestInfo);
            
        }else if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_BATCH_RECHARGE){
            
        }else if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_MOVE_STOCK){
            
        }else if( old_transactionRecord.type==TransactionRecord.TRANS_TYPE_ADD_BALANCE){
            
        }
        
	}
	private void onRefundRecharge(RequestInfo requestInfo){
	    AgentRequest agentRequest=requestInfo.agentRequest;
	    TransactionRecord old_transactionRecord=requestInfo.old_transactionRecord;
	    DealerInfo dealerInfo=null;
        try {
            dealerInfo = connection.getDealerInfo(old_transactionRecord.dealer_id);
        } catch (SQLException e) {
            isConnected = false;
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_description = MySQLConnection.getSQLExceptionString(e);
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            return;
        }
        if(dealerInfo==null){
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_description = "CONTENT_DEALER_NOT_FOUND";
            logError(agentRequest.getRespString());
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: dealer not found.");
            return;
        }
        TransactionRecord transactionRecord = createTransactionRecord();
        requestInfo.transactionRecord= requestInfo.transactionRecord;
        transactionRecord.type = TransactionRecord.TRANS_TYPE_REFUND_RECHARGE;
        transactionRecord.dealer_msisdn = old_transactionRecord.dealer_msisdn;
        transactionRecord.dealer_id = old_transactionRecord.dealer_id;
        transactionRecord.balance_before = dealerInfo.balance;
        transactionRecord.balance_changed_amount = -1*old_transactionRecord.balance_changed_amount;
       // transactionRecord.recharge_msidn = old_transactionRecord.recharge_msidn;
        //transactionRecord.recharge_value = rechargeCmd.amount;
        transactionRecord.agent = agentRequest.agent_username;
        transactionRecord.agent_id = agentRequest.agent_id;
        transactionRecord.cash_value = agentRequest.cash_value;
        transactionRecord.invoice_code = agentRequest.invoice_code;
        
        GetSubInfoCmd getSubInfoCmd = new GetSubInfoCmd();
        getSubInfoCmd.msisdn = requestInfo.msisdn;
        getSubInfoCmd.transactionId = getPaymentGWTransactionId();
        getSubInfoCmd.reqDate = new Date(System.currentTimeMillis());
        getSubInfoCmd.rechargeMsisdn = old_transactionRecord.recharge_msidn;
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
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
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
								.replaceAll("<TRANS_ID>", ""+transactionRecord.id);
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
				listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				return;
			}
		}
		else{
			transactionRecord.balance_after = requestInfo.dealerInfo.balance;
			transactionRecord.type = TransactionRecord.TRANS_TYPE_RECHARGE;
			transactionRecord.balance_changed_amount = -1*rechargeCmd.amount;
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
			batchRechargeElement.status = BatchRechargeElement.STATUS_SUCCESS;
			batchRechargeElement.result_code = paymentPostpaidCmdResp.resultCode;
			batchRechargeElement.result_string = paymentPostpaidCmdResp.resultString;
			updateBatchRechargeElement(batchRechargeElement);
			transactionRecord.balance_changed_amount = transactionRecord.balance_changed_amount-batchRechargeElement.recharge_value;
			try {
				connection.deductBalance(batchRechargeCmd.currentBatchRechargeElement);
				if(batchRechargeCmd.currentBatchRechargeElement.db_return_code==0){
					transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
					transactionRecord.balance_after = batchRechargeCmd.currentBatchRechargeElement.balanceAfter;
					transactionRecord.recharge_value += batchRechargeCmd.currentBatchRechargeElement.recharge_value;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
					transactionRecord.result_description = "Batch recharge success";
					batchRechargeCmd.resultCode = RequestCmd.R_OK;
					batchRechargeCmd.resultString = "Success";
					logInfo(batchRechargeCmd.getRespString());
					String content1 = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<DATE_TIME>", getDateTimeFormated(transactionRecord.date_time))
								.replaceAll("<AMOUNT>", ""+batchRechargeCmd.batch_recharge_total_amount)
								.replaceAll("<DEALER_NUMBER> ", batchRechargeCmd.msisdn.replaceFirst("856", "0"));
						String ussdContent1 = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_RECHARGE_SUCCESS_NOTIFY")
								.replaceAll("<AMOUNT>", ""+batchRechargeCmd.batch_recharge_total_amount)
								.replaceAll("<DEALER_NUMBER> ", batchRechargeCmd.msisdn.replaceFirst("856", "0"));
						
						sendSms("856"+batchRechargeCmd.currentBatchRechargeElement.recharge_msisdn, content1, ussdContent1, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
						dealerRequest.result = "CONTENT_RECHARGE_OTHER_SUCCESS";
					
					updateDealerRequest(requestInfo.dealerRequest);
					listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
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
				listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
				return;
			}
		}
		else{
			//TransactionRecord transactionRecord = createTransactionRecord();
			transactionRecord.dealer_msisdn = requestInfo.dealerInfo.msisdn;
			transactionRecord.dealer_id = requestInfo.dealerInfo.id;
			transactionRecord.balance_before = requestInfo.dealerInfo.balance;
			transactionRecord.balance_after = requestInfo.dealerInfo.balance;
			transactionRecord.type = TransactionRecord.TRANS_TYPE_RECHARGE;
			transactionRecord.balance_changed_amount = -1*batchRechargeCmd.batch_recharge_total_amount;
			transactionRecord.batch_recharge_id = batchRechargeCmd.batch_recharge_id;
			//transactionRecord.recharge_value = batchRechargeCmd.amount;
			batchRechargeCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
			batchRechargeCmd.resultString = "Payment Postpaid subscriber failed";
			transactionRecord.result_description = batchRechargeCmd.resultString;
			logInfo(requestInfo.dealerRequest.requestCmd.getRespString());
			requestInfo.dealerRequest.result = "CONTENT_PAYMENT_POSTPAID_FAILED";
			transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
			transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;

			insertTransactionRecord(transactionRecord);
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_PAYMENT_POSTPAID_FAILED");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_PAYMENT_POSTPAID_FAILED");
			sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_RECHARGE, transactionRecord.id);
			requestInfo.dealerRequest.dealer_id = requestInfo.dealerInfo.id;
			requestInfo.dealerRequest.transaction_id = transactionRecord.id;
			updateDealerRequest(requestInfo.dealerRequest);
			listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
		}
	}
	
	private void OnTopupPrepaidCmdResp(TopupPrepaidCmd topupPrepaidCmdResp) {
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
					transactionRecord.balance_changed_amount = -1*rechargeCmd.amount;
					transactionRecord.balance_after = rechargeCmd.balanceAfter;
					transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_PREPAID;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
					transactionRecord.result_description = "Topup Prepaid subscriber success";
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
			listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
		}
	}

	private void OnGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp) {
		// TODO Auto-generated method stub
		logInfo(getSubInfoCmdResp.getRespString());
		RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
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
		if(getSubInfoCmdResp.resultCode==PaymentGWResultCode.RC_GET_SUBS_INFO_SUCCESS){
			if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_PREPAID){
				TopupPrepaidCmd topupPrepaidCmd = new TopupPrepaidCmd();
				topupPrepaidCmd.msisdn = getSubInfoCmdResp.msisdn;
				topupPrepaidCmd.transactionId = getPaymentGWTransactionId();
				BatchRechargeElement batchRechargeElement = batchRechargeCmd.currentBatchRechargeElement;
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
				BatchRechargeElement batchRechargeElement = batchRechargeCmd.currentBatchRechargeElement;
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
			TransactionRecord transactionRecord = requestInfo.transactionRecord;
			BatchRechargeElement batchRechargeElement = batchRechargeCmd.currentBatchRechargeElement;
			RechargeCdrRecord rechargeCdrRecord = new RechargeCdrRecord();
			rechargeCdrRecord.type = RechargeCdrRecord.TYPE_BATCH_RECHARGE;
			rechargeCdrRecord.payment_transaction_id = getSubInfoCmdResp.transactionId;
			rechargeCdrRecord.balance_before = requestInfo.dealerInfo.balance;
			rechargeCdrRecord.balance_after = requestInfo.dealerInfo.balance;
			rechargeCdrRecord.balance_changed_amount = -1*batchRechargeElement.recharge_value;
			rechargeCdrRecord.date_time = new Timestamp(System.currentTimeMillis());
			rechargeCdrRecord.dealer_id = requestInfo.dealerInfo.id;
			rechargeCdrRecord.dealer_msisdn = requestInfo.msisdn;
			rechargeCdrRecord.payment_transaction_id = getSubInfoCmdResp.transactionId;
			rechargeCdrRecord.receiver_msidn = batchRechargeElement.recharge_msisdn;
			rechargeCdrRecord.receiver_sub_type = 0;
			rechargeCdrRecord.recharge_value = batchRechargeElement.recharge_value;
			rechargeCdrRecord.result = RechargeCdrRecord.STATUS_FAILED;
			rechargeCdrRecord.result_code = getSubInfoCmdResp.resultCode;
			rechargeCdrRecord.result_description = "Get subinfo failed:"+getSubInfoCmdResp.resultString;
			insertRechargeCdrRecord(rechargeCdrRecord);
			
			batchRechargeCmd.recharge_failed++;
			batchRechargeElement.status = BatchRechargeElement.STATUS_FAILED;
			batchRechargeElement.result_code = getSubInfoCmdResp.resultCode;
			batchRechargeElement.result_string = getSubInfoCmdResp.resultString;
			updateBatchRechargeElement(batchRechargeElement);

			if(batchRechargeCmd.batchRechargeElements.isEmpty()){
				logInfo(requestInfo.dealerRequest.requestCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_GETS_SUBINFO_FAILED");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_GETS_SUBINFO_FAILED");
				sendSms(requestInfo.dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
				requestInfo.dealerRequest.result = "CONTENT_GETS_SUBINFO_FAILED";
				updateDealerRequest(requestInfo.dealerRequest);
				transactionRecord.balance_after = requestInfo.dealerInfo.balance;
				transactionRecord.result_description = batchRechargeCmd.getResultDescription();
				transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
				insertTransactionRecord(transactionRecord);
				listRequestProcessing.remove(requestInfo.dealerRequest.msisdn);
			}
			else{
				batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
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
		}
	}
	private void OnRefundGetSubInfoResp(GetSubInfoCmd getSubInfoCmdResp){
        logInfo(getSubInfoCmdResp.getRespString());
        RequestInfo requestInfo = listRequestProcessing.get(getSubInfoCmdResp.msisdn);
        if(requestInfo.old_transactionRecord.type==TransactionRecord.TRANS_TYPE_RECHARGE){
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
                int subBalance=5000; 
                if(subBalance >= Config.MULTIPLIER ){
                    
                }else{
                    transactionRecord.recharge_sub_type = 0;
                    transactionRecord.balance_after =   transactionRecord.balance_before;
                    transactionRecord.result_description = "Get SubInfo Failed";
                    transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
                    insertTransactionRecord(transactionRecord);
                    
                    agentRequest.status = AgentRequest.STATUS_FAILED;
                    agentRequest.result_description = "CONTENT_GETS_SUBINFO_FAILED";
                    updateAgentRequest(agentRequest);
                    listRequestProcessing.remove(requestInfo.msisdn);
                    logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Get SubInfo Failed");
                    return;
                }
                
                ChargingCmd chargingCmd = new ChargingCmd();
                chargingCmd.msisdn=requestInfo.msisdn;
                chargingCmd.recharge_msidn=requestInfo.old_transactionRecord.recharge_msidn;
                
                chargingCmd.transactionID=getPaymentGWTransactionId();
                chargingCmd.spID=Config.charging_spID;
                chargingCmd.serviceID=Config.charging_serviceID;
                chargingCmd.chargeValue=requestInfo.old_transactionRecord.balance_changed_amount;
                ChargingSession chargingSession = new ChargingSession(chargingCmd, queueChargingCmdResp, logger);
                chargingSession.start();
                
                
            }
            else if(getSubInfoCmdResp.subType == GetSubInfoCmd.SUBS_TYPE_POSTPAID){

                agentRequest.balance_add_amount = requestInfo.old_transactionRecord.balance_changed_amount;
                agentRequest.dealer_id = requestInfo.old_transactionRecord.dealer_id;
                updateDealer(agentRequest);
                
                transactionRecord.recharge_sub_type = GetSubInfoCmd.SUBS_TYPE_POSTPAID;

                transactionRecord.date_time = new Timestamp(System.currentTimeMillis());
                transactionRecord.balance_after = transactionRecord.balance_before+ agentRequest.balance_add_amount ;
                transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
                transactionRecord.result_description = "Refund recharge postpaid subscriber success";
                insertTransactionRecord(transactionRecord);
                
                agentRequest.status = AgentRequest.STATUS_SUCCESS;
                agentRequest.result_description = "CONTENT_REFUND_RECHARGE_SUCCESS";
                updateAgentRequest(agentRequest);
                listRequestProcessing.remove(requestInfo.msisdn);
                logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +" success.;");
            }
        }
        else{
            transactionRecord.recharge_sub_type = 0;
            transactionRecord.balance_after =   transactionRecord.balance_before;
            transactionRecord.result_description = "Get SubInfo Failed";
            transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
            insertTransactionRecord(transactionRecord);
            
            agentRequest.status = AgentRequest.STATUS_FAILED;
            agentRequest.result_description = "CONTENT_GETS_SUBINFO_FAILED";
            updateAgentRequest(agentRequest);
            listRequestProcessing.remove(requestInfo.msisdn);
            logInfo("Refun transaction : id:"+requestInfo.agentRequest.transaction_id +"; error: Get SubInfo Failed");
            return;
        }
    }
	private void updateBatchRechargeElement(BatchRechargeElement batchRechargeElement) {
		// TODO Auto-generated method stub
		
	}

	private void insertRechargeCdrRecord(RechargeCdrRecord rechargeCdrRecord) {
		// TODO Auto-generated method stub
		
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
			batchRechargeCmd.resultString = "Account has not enought balance";
			
			logInfo(batchRechargeCmd.getRespString());
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BALANCE_NOT_ENOUGHT")
					.replaceAll("<BALANCE>", ""+dealerInfo.balance);
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BALANCE_NOT_ENOUGHT").replaceAll("<BALANCE>", ""+dealerInfo.balance);
			sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_BATCH_RECHARGE, 0);
			dealerRequest.result = "CONTENT_BALANCE_NOT_ENOUGHT";
			updateDealerRequest(dealerRequest);
			listRequestProcessing.remove(dealerRequest.msisdn);
		}
		else{
			batchRechargeCmd.currentBatchRechargeElement = batchRechargeCmd.batchRechargeElements.remove(0);
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
			dealerRequest.dealer_id = dealerInfo.id;
			dealerRequest.transaction_id = transactionRecord.id;
			transactionRecord.status = TransactionRecord.TRANS_STATUS_SUCCESS;
			transactionRecord.result_description = "Batch recharge success";
			
			batchRechargeCmd.resultCode = BatchRechargeCmd.R_TELCO_ERROR;
			batchRechargeCmd.resultString = "Batch recharge failed";
			
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
		transactionRecord.recharge_msidn = rechargeCmd.rechargeMsisdn;
		transactionRecord.recharge_value = rechargeCmd.amount;
		dealerRequest.dealer_id = dealerInfo.id;
		dealerRequest.transaction_id = transactionRecord.id;
		
		boolean isValidPIN = dealerInfo.pin_code.equals(rechargeCmd.pinCode);
		boolean isValidAmount = (rechargeCmd.amount>=1000&&rechargeCmd.amount%1000==0)?true:false;
		
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
				rechargeCmd.resultString = "Account has not enought balance";
				content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BALANCE_NOT_ENOUGHT")
						.replaceAll("<BALANCE>", ""+dealerInfo.balance);
				ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BALANCE_NOT_ENOUGHT").replaceAll("<BALANCE>", ""+dealerInfo.balance);
				dealerRequest.result = "CONTENT_BALANCE_NOT_ENOUGHT";
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
		if(isDealer(dealerRequest, SmsTypes.SMS_TYPE_MOVE_STOCK))
			return;
		DealerInfo dealerInfo = moveStockCmd.dealerInfo;
		DealerInfo receiverInfo = null;

		TransactionRecord transactionRecord = createTransactionRecord();
		transactionRecord.type = TransactionRecord.TRANS_TYPE_MOVE_STOCK;
		transactionRecord.dealer_msisdn = dealerInfo.msisdn;
		transactionRecord.dealer_id = dealerInfo.id;
		transactionRecord.balance_before = dealerInfo.balance;
		transactionRecord.balance_changed_amount = -1*moveStockCmd.amount;
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

		boolean isValidPIN = dealerInfo.pin_code.equals(moveStockCmd.pinCode);
		boolean isValidAmount = (moveStockCmd.amount>=1000&&moveStockCmd.amount%1000==0)?true:false;
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
			if(receiverInfo!=null){
				transactionRecord.partner_msisdn = receiverInfo.msisdn;
				transactionRecord.partner_id = receiverInfo.id;
				transactionRecord.partner_balance_before = receiverInfo.balance;

				if(moveStockCmd.amount>dealerInfo.balance){
					moveStockCmd.resultCode = RequestCmd.R_CUSTOMER_INFO_FAIL;
					moveStockCmd.resultString = "Account has not enought balance";
					logInfo(moveStockCmd.getRespString());
					transactionRecord.balance_after = dealerInfo.balance;
					transactionRecord.partner_balance_after = receiverInfo.balance;
					transactionRecord.status = TransactionRecord.TRANS_STATUS_FAILED;
					transactionRecord.result_description = moveStockCmd.resultString;
					String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_BALANCE_NOT_ENOUGHT").replaceAll("<BALANCE>", ""+dealerInfo.balance);
					String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_BALANCE_NOT_ENOUGHT");
					sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, transactionRecord.id);
					dealerRequest.result = "CONTENT_BALANCE_NOT_ENOUGHT";
					dealerRequest.dealer_id = dealerInfo.id;
					dealerRequest.transaction_id = transactionRecord.id;
					insertTransactionRecord(transactionRecord);
				}
				else{
					try {
						connection.moveStock(moveStockCmd);
						if(moveStockCmd.db_return_code==0){
							
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
									.replaceAll("<TRANS_ID>", ""+transactionRecord.id);
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
				moveStockCmd.resultString = "The receiver is not a dealer";
				logInfo(moveStockCmd.getRespString());
				String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_RECEIVER_NOT_IS_DEALER");
				String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_RECEIVER_NOT_IS_DEALER");
				sendSms(dealerRequest.msisdn, content, ussdContent, SmsTypes.SMS_TYPE_MOVE_STOCK, 0);
				dealerRequest.result = "CONTENT_RECEIVER_NOT_IS_DEALER";
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
			String content = Config.smsMessageContents[Config.smsLanguage].getParam("CONTENT_NOT_IS_DEALER");
			String ussdContent = Config.ussdMessageContents[Config.smsLanguage].getParam("NOTIFY_NOT_IS_DEALER");
			sendSms(dealerRequest.msisdn, content, ussdContent, smsType, 0);
			dealerRequest.result = "CONTENT_NOT_IS_DEALER";
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
