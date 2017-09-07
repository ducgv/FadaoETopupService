/**
 * 
 */
package com.itpro.paymentgw;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.itpro.paymentgw.cmd.GetSubInfoCmd;
import com.itpro.paymentgw.cmd.KeepAliveCmd;
import com.itpro.paymentgw.cmd.LoginCmd;
import com.itpro.paymentgw.cmd.PaymentGWCmd;
import com.itpro.paymentgw.cmd.PaymentPostpaidCmd;
import com.itpro.paymentgw.cmd.TopupPrepaidCmd;
import com.itpro.util.ProcessingThread;
import com.itpro.util.Queue;
import com.topup.payment.TopupPaymentApiWS;
import com.topup.payment.TopupPaymentApiWSPortType;
import com.topup.payment.xsd.TopupPaymentApiWSKeepAliveResult;
import com.topup.payment.xsd.TopupPaymentApiWSLoginResult;
import com.topup.payment.xsd.TopupPaymentApiWSPaymentPospaidResult;
import com.topup.payment.xsd.TopupPaymentApiWSPaymentPostpaidHeader;
import com.topup.payment.xsd.TopupPaymentApiWSQeuryProfileHeader;
import com.topup.payment.xsd.TopupPaymentApiWSQeuryProfilefoResult;
import com.topup.payment.xsd.TopupPaymentApiWSTopupPrepaidHeader;
import com.topup.payment.xsd.TopupPaymentApiWSTopupPrepaidResult;

/**
 * @author Giap Van Duc
 *
 */
public class PaymentGWSession extends ProcessingThread {
	public PaymentGWSession(PaymentGWCmd userCmd, Queue queueResp) {
		// TODO Auto-generated constructor stub
		//serviceLocator = new Vasgateway_ServiceLocator();
		this.userCmd = userCmd;
		this.queueResp = queueResp;
	}

	//private Vasgateway_ServiceLocator serviceLocator;
	private PaymentGWCmd userCmd;
	private Queue queueResp;
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
		setLogPrefix("[PaymentGWSession] ");
	}

	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#process()
	 */
	@Override
	protected void process() {
		// TODO Auto-generated method stub
		if (userCmd instanceof LoginCmd) {
			LoginCmd loginCmd = (LoginCmd)userCmd;
			OnLoginCmd(loginCmd);
		}
		else if (userCmd instanceof KeepAliveCmd) {
			KeepAliveCmd keepAliveCmd = (KeepAliveCmd)userCmd;
			OnKeepAliveCmd(keepAliveCmd);
		}
		else if (userCmd instanceof GetSubInfoCmd) {
			GetSubInfoCmd getSubInfoCmd = (GetSubInfoCmd)userCmd;
			OnGetSubInfoCmd(getSubInfoCmd);
		}
		else if (userCmd instanceof TopupPrepaidCmd) {
			TopupPrepaidCmd topupPrepaidCmd = (TopupPrepaidCmd)userCmd;
			OnTopupPrepaidCmd(topupPrepaidCmd);
		}
		else if (userCmd instanceof PaymentPostpaidCmd) {
			PaymentPostpaidCmd paymentPostpaidCmd = (PaymentPostpaidCmd)userCmd;
			OnPaymentPostpaidCmd(paymentPostpaidCmd);
		}
		stop();
	}

	private void OnPaymentPostpaidCmd(PaymentPostpaidCmd paymentPostpaidCmd) {
		// TODO Auto-generated method stub
		logInfo(paymentPostpaidCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = null;
		TopupPaymentApiWSPortType service = null;
		TopupPaymentApiWSPaymentPospaidResult result = null;
		try {
			topupPaymentApiWS = new TopupPaymentApiWS();
			service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
			result = service.paymentPostpaid(paymentPostpaidCmd.rechargeMsisdn, ""+paymentPostpaidCmd.amount, ""+paymentPostpaidCmd.transactionId, (new SimpleDateFormat("yyyyMMdd")).format(paymentPostpaidCmd.reqDate), ""+paymentPostpaidCmd.balanceBonus, ""+paymentPostpaidCmd.dataBonus, paymentPostpaidCmd.originalNumber, paymentPostpaidCmd.token);
		} catch (Exception e) {
			// TODO: handle exception
			paymentPostpaidCmd.result = PaymentGWResultCode.R_ERROR;
			paymentPostpaidCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			paymentPostpaidCmd.resultString=e.getMessage();
			logInfo(paymentPostpaidCmd.getRespString());
			queueResp.enqueue(paymentPostpaidCmd);
			return;
		}

		paymentPostpaidCmd.result = PaymentGWResultCode.R_SUCCESS;
		try {
			paymentPostpaidCmd.advanceBalance = result.getMsisdnAdvanceBalance().isNil()?0:Integer.parseInt(result.getMsisdnAdvanceBalance().getValue());
		} catch (Exception e) {
			// TODO: handle exception
			logWarning("OnPaymentPostpaidCmd: Error when parse MsisdnAdvanceBalance field of msisdn "+paymentPostpaidCmd.msisdn);
			paymentPostpaidCmd.advanceBalance = 0;
		}
		
		try {
			paymentPostpaidCmd.debitBalance = result.getMsisdnDebitBalance().isNil()?0:Integer.parseInt(result.getMsisdnDebitBalance().getValue());
		} catch (Exception e) {
			// TODO: handle exception
			logWarning("OnPaymentPostpaidCmd: Error when parse MsisdnDebitBalance field of msisdn "+paymentPostpaidCmd.msisdn);
			paymentPostpaidCmd.debitBalance = 0;
		}
		
		TopupPaymentApiWSPaymentPostpaidHeader header = result.getPaymentPostpaidHeader().isNil()?null:result.getPaymentPostpaidHeader().getValue();
		if(header!=null){
			try {
				paymentPostpaidCmd.resultCode=header.getResultcode().isNil()?PaymentGWResultCode.RC_PAYMENTGW_RESULT_NULL:Integer.parseInt(header.getResultcode().getValue());
				paymentPostpaidCmd.resultString=header.getResultDes().getValue();
			} catch (Exception e) {
				// TODO: handle exception
				paymentPostpaidCmd.resultCode = PaymentGWResultCode.RC_PAYMENTGW_RESULT_WRONG_FORMAT;
				paymentPostpaidCmd.resultString = "PaymentGW ResultCode:"+header.getResultcode().getValue();
				paymentPostpaidCmd.result = PaymentGWResultCode.R_ERROR;
			}
			
		}
		else{
			paymentPostpaidCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			paymentPostpaidCmd.resultString=PaymentGWResultCode.resultDesc.get(PaymentGWResultCode.RC_CALL_SOAP_ERROR);
			paymentPostpaidCmd.result = PaymentGWResultCode.R_ERROR;
		}

		logInfo(paymentPostpaidCmd.getRespString());
		queueResp.enqueue(paymentPostpaidCmd);
	}

	private void OnTopupPrepaidCmd(TopupPrepaidCmd topupPrepaidCmd) {
		// TODO Auto-generated method stub
		logInfo(topupPrepaidCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = null;
		TopupPaymentApiWSPortType service = null;
		TopupPaymentApiWSTopupPrepaidResult result = null;
		try {
			topupPaymentApiWS = new TopupPaymentApiWS();
			service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
			result = service.topupPrepaid(topupPrepaidCmd.rechargeMsisdn, ""+topupPrepaidCmd.amount, ""+topupPrepaidCmd.transactionId, (new SimpleDateFormat("yyyyMMdd")).format(topupPrepaidCmd.reqDate), ""+topupPrepaidCmd.balanceBonus, ""+topupPrepaidCmd.dataBonus, topupPrepaidCmd.originalNumber, topupPrepaidCmd.token);
		} catch (Exception e) {
			// TODO: handle exception
			topupPrepaidCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			topupPrepaidCmd.resultString=e.getMessage();
			logInfo(topupPrepaidCmd.getRespString());
			queueResp.enqueue(topupPrepaidCmd);
			return;
		}

		topupPrepaidCmd.result = PaymentGWResultCode.R_SUCCESS;
		try {
			topupPrepaidCmd.currentBalance = result.getTargetCurrentBalance().isNil()?0:Integer.parseInt(result.getTargetCurrentBalance().getValue());
		} catch (Exception e) {
			// TODO: handle exception
			logWarning("OnTopupPrepaidCmd: Error when parse TargetCurrentBalance field of msisdn "+topupPrepaidCmd.msisdn);
			topupPrepaidCmd.currentBalance = 0;
		}
		
		try {
			topupPrepaidCmd.newActiveDate = result.getTargetNewActivedate().isNil()?null:(new SimpleDateFormat("yyyyMMdd").parse(result.getTargetNewActivedate().getValue()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logWarning("OnTopupPrepaidCmd: Error when parse TargetNewActivedate field of msisdn "+topupPrepaidCmd.msisdn);
			topupPrepaidCmd.newActiveDate = null;
		}
		TopupPaymentApiWSTopupPrepaidHeader header = result.getTopupMasterSimPrepaidHeader().isNil()?null:result.getTopupMasterSimPrepaidHeader().getValue();
		if(header!=null){
			try {
				topupPrepaidCmd.resultCode=header.getResultcode().isNil()?PaymentGWResultCode.RC_PAYMENTGW_RESULT_NULL:Integer.parseInt(header.getResultcode().getValue());
				topupPrepaidCmd.resultString=header.getResultDes().getValue();
			} catch (Exception e) {
				// TODO: handle exception
				topupPrepaidCmd.resultCode = PaymentGWResultCode.RC_PAYMENTGW_RESULT_WRONG_FORMAT;
				topupPrepaidCmd.resultString = "PaymentGW ResultCode:"+header.getResultcode().getValue();
				topupPrepaidCmd.result = PaymentGWResultCode.R_ERROR;
			}
		}
		else{
			topupPrepaidCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			topupPrepaidCmd.resultString=PaymentGWResultCode.resultDesc.get(PaymentGWResultCode.RC_CALL_SOAP_ERROR);
			topupPrepaidCmd.result = PaymentGWResultCode.R_ERROR;
		}

		logInfo(topupPrepaidCmd.getRespString());
		queueResp.enqueue(topupPrepaidCmd);
	}

	private void OnGetSubInfoCmd(GetSubInfoCmd getSubInfoCmd) {
		// TODO Auto-generated method stub
		logInfo(getSubInfoCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = null;
		TopupPaymentApiWSPortType service = null;
		TopupPaymentApiWSQeuryProfilefoResult result = null;
		try {
			topupPaymentApiWS = new TopupPaymentApiWS();
			service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
			result = service.qeuryProfileSubcriber(getSubInfoCmd.rechargeMsisdn, ""+getSubInfoCmd.transactionId, (new SimpleDateFormat("yyyyMMdd")).format(getSubInfoCmd.reqDate), getSubInfoCmd.token);
		} catch (Exception e) {
			// TODO: handle exception
			getSubInfoCmd.activeDate = null;
			getSubInfoCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			getSubInfoCmd.resultString=e.getMessage();
			logInfo(getSubInfoCmd.getRespString());
			queueResp.enqueue(getSubInfoCmd);
			return;
		}

		getSubInfoCmd.result = PaymentGWResultCode.R_SUCCESS;
		try{
			getSubInfoCmd.subType = result.getPayType().isNil()?-1:Integer.parseInt(result.getPayType().getValue());
		}
		catch(NumberFormatException e){
			getSubInfoCmd.subType = -1;
		}

		try {
			getSubInfoCmd.activeDate = result.getActiveDate().isNil()?null:(new SimpleDateFormat("yyyyMMdd").parse(result.getActiveDate().getValue()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			getSubInfoCmd.activeDate = null;
			getSubInfoCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			getSubInfoCmd.resultString="Wrong ActiveDate:"+result.getActiveDate().getValue();
			logInfo(getSubInfoCmd.getRespString());
			queueResp.enqueue(getSubInfoCmd);
			return;

		}
		try{
			getSubInfoCmd.balance = result.getPpsBalance().isNil()?0:Integer.parseInt(result.getPpsBalance().getValue());
		}
		catch(NumberFormatException e){
			getSubInfoCmd.balance = 0;
			if(getSubInfoCmd.subType == GetSubInfoCmd.SUBS_TYPE_PREPAID){
				getSubInfoCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
				getSubInfoCmd.resultString="Wrong Balance:"+result.getPpsBalance().getValue();
				logInfo(getSubInfoCmd.getRespString());
				queueResp.enqueue(getSubInfoCmd);
				return;
			}
		}
		/*
		getSubInfoCmd.subId = result.getSubID().isNil()?"":result.getSubID().getValue();
		if(getSubInfoCmd.subId == null|| getSubInfoCmd.subId.equals("")){
			getSubInfoCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			getSubInfoCmd.resultString="SubID is NULL";
			logInfo(getSubInfoCmd.getRespString());
			queueResp.enqueue(getSubInfoCmd);
			return;
		}
		 */
		TopupPaymentApiWSQeuryProfileHeader header = result.getQeuryBasicInfoHeader().isNil()?null:result.getQeuryBasicInfoHeader().getValue();
		if(header!=null){
			getSubInfoCmd.resultCode=Integer.parseInt(header.getResultcode().getValue());
			getSubInfoCmd.resultString=header.getResultDes().getValue();
		}
		else{
			getSubInfoCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			getSubInfoCmd.resultString=PaymentGWResultCode.resultDesc.get(PaymentGWResultCode.RC_CALL_SOAP_ERROR);
		}

		logInfo(getSubInfoCmd.getRespString());
		queueResp.enqueue(getSubInfoCmd);
	}

	private void OnLoginCmd(LoginCmd loginCmd) {
		// TODO Auto-generated method stub
		logInfo(loginCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = null;
		TopupPaymentApiWSPortType service = null;
		TopupPaymentApiWSLoginResult result = null;
		try {
			topupPaymentApiWS = new TopupPaymentApiWS();
			service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
			result = service.loginWS(loginCmd.spID, loginCmd.spPassword, ""+loginCmd.transactionId);
		} catch (Exception e) {
			// TODO: handle exception
			loginCmd.result = PaymentGWResultCode.R_ERROR;
			loginCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			loginCmd.resultString=e.getMessage();
			logInfo(loginCmd.getRespString());
			queueResp.enqueue(loginCmd);
			return;
		}

		loginCmd.result = PaymentGWResultCode.R_SUCCESS;
		try{
			loginCmd.resultCode=Integer.parseInt(result.getResultcode().getValue());
		}
		catch(Exception e){
			loginCmd.result = PaymentGWResultCode.R_ERROR;
			loginCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			loginCmd.resultString=PaymentGWResultCode.resultDesc.get(PaymentGWResultCode.RC_CALL_SOAP_ERROR);
			logError(loginCmd.getRespString());
			queueResp.enqueue(loginCmd);
			return;
		}
		loginCmd.resultString=result.getResultDescrib().getValue();
		loginCmd.token=result.getToken().getValue();
		logInfo(loginCmd.getRespString());
		queueResp.enqueue(loginCmd);
	}

	private void OnKeepAliveCmd(KeepAliveCmd keepAliveCmd) {
		// TODO Auto-generated method stub
		logInfo(keepAliveCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = null;
		TopupPaymentApiWSPortType service = null;
		TopupPaymentApiWSKeepAliveResult result = null;
		try {
			topupPaymentApiWS = new TopupPaymentApiWS();
			service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
			result = service.keepalive(keepAliveCmd.token);
		} catch (Exception e) {
			// TODO: handle exception
			keepAliveCmd.result = PaymentGWResultCode.R_ERROR;
			keepAliveCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			keepAliveCmd.resultString=e.getMessage();
			logError(keepAliveCmd.getRespString());
			queueResp.enqueue(keepAliveCmd);
			return;
		}

		keepAliveCmd.result = PaymentGWResultCode.R_SUCCESS;
		try{
			keepAliveCmd.resultCode=Integer.parseInt(result.getResultcode().getValue());
			keepAliveCmd.resultString=result.getResultDes().isNil()?"NULL value":result.getResultDes().getValue();
		}
		catch(Exception e){
			keepAliveCmd.result = PaymentGWResultCode.R_ERROR;
			keepAliveCmd.resultCode=PaymentGWResultCode.RC_CALL_SOAP_ERROR;
			keepAliveCmd.resultString=PaymentGWResultCode.resultDesc.get(PaymentGWResultCode.RC_CALL_SOAP_ERROR);
			logError(keepAliveCmd.getRespString());
			queueResp.enqueue(keepAliveCmd);
			return;
		}
		
		logInfo(keepAliveCmd.getRespString());
		queueResp.enqueue(keepAliveCmd);
	}
}
