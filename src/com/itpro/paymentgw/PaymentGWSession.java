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
		TopupPaymentApiWS topupPaymentApiWS = new TopupPaymentApiWS();
		TopupPaymentApiWSPortType service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
		TopupPaymentApiWSPaymentPospaidResult result = service.paymentPostpaid(paymentPostpaidCmd.rechargeMsisdn, ""+paymentPostpaidCmd.amount, ""+paymentPostpaidCmd.transactionId, (new SimpleDateFormat("yyyyMMdd")).format(paymentPostpaidCmd.reqDate), paymentPostpaidCmd.token);
		paymentPostpaidCmd.result = PaymentGWResultCode.R_SUCCESS;
		paymentPostpaidCmd.advanceBalance = result.getMsisdnAdvanceBalance().isNil()?0:Integer.parseInt(result.getMsisdnAdvanceBalance().getValue());
		paymentPostpaidCmd.debitBalance = result.getMsisdnDebitBalance().isNil()?0:Integer.parseInt(result.getMsisdnDebitBalance().getValue());
		TopupPaymentApiWSPaymentPostpaidHeader header = result.getPaymentPostpaidHeader().isNil()?null:result.getPaymentPostpaidHeader().getValue();
		if(header!=null){
			paymentPostpaidCmd.resultCode=Integer.parseInt(header.getResultcode().getValue());
			paymentPostpaidCmd.resultString=header.getResultDes().getValue();
		}
		else{
			paymentPostpaidCmd.resultCode=-1;
			paymentPostpaidCmd.resultString="Call API function error";
		}
		
		logInfo(paymentPostpaidCmd.getRespString());
		queueResp.enqueue(paymentPostpaidCmd);
	}

	private void OnTopupPrepaidCmd(TopupPrepaidCmd topupPrepaidCmd) {
		// TODO Auto-generated method stub
		logInfo(topupPrepaidCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = new TopupPaymentApiWS();
		TopupPaymentApiWSPortType service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
		TopupPaymentApiWSTopupPrepaidResult result = service.topupPrepaid(topupPrepaidCmd.msisdn, ""+topupPrepaidCmd.amount, ""+topupPrepaidCmd.transactionId, (new SimpleDateFormat("yyyyMMdd")).format(topupPrepaidCmd.reqDate), topupPrepaidCmd.token);
		topupPrepaidCmd.result = PaymentGWResultCode.R_SUCCESS;
		topupPrepaidCmd.currentBalance = result.getTargetCurrentBalance().isNil()?0:Integer.parseInt(result.getTargetCurrentBalance().getValue());
		try {
			topupPrepaidCmd.newActiveDate = result.getTargetNewActivedate().isNil()?null:(new SimpleDateFormat("yyyyMMdd").parse(result.getTargetNewActivedate().getValue()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logError("OnTopupPrepaidCmd: Error when parse newActiveDate field of msisdn "+topupPrepaidCmd.msisdn);
			topupPrepaidCmd.newActiveDate = null;
		}
		TopupPaymentApiWSTopupPrepaidHeader header = result.getTopupMasterSimPrepaidHeader().isNil()?null:result.getTopupMasterSimPrepaidHeader().getValue();
		if(header!=null){
			topupPrepaidCmd.resultCode=Integer.parseInt(header.getResultcode().getValue());
			topupPrepaidCmd.resultString=header.getResultDes().getValue();
		}
		else{
			topupPrepaidCmd.resultCode=-1;
			topupPrepaidCmd.resultString="Call API function error";
		}
		
		logInfo(topupPrepaidCmd.getRespString());
		queueResp.enqueue(topupPrepaidCmd);
	}

	private void OnGetSubInfoCmd(GetSubInfoCmd getSubInfoCmd) {
		// TODO Auto-generated method stub
		logInfo(getSubInfoCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = new TopupPaymentApiWS();
		TopupPaymentApiWSPortType service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
		TopupPaymentApiWSQeuryProfilefoResult result = service.qeuryProfileSubcriber(getSubInfoCmd.msisdn, ""+getSubInfoCmd.transactionId, (new SimpleDateFormat("yyyyMMdd")).format(getSubInfoCmd.reqDate), getSubInfoCmd.token);
		getSubInfoCmd.result = PaymentGWResultCode.R_SUCCESS;
		getSubInfoCmd.subType = result.getPayType().isNil()?-1:Integer.parseInt(result.getPayType().getValue());
		try {
			getSubInfoCmd.activeDate = result.getActiveDate().isNil()?null:(new SimpleDateFormat("yyyyMMdd").parse(result.getActiveDate().getValue()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logError("OnGetSubInfoCmd: Error when parse activeDate field of msisdn "+getSubInfoCmd.msisdn);
			getSubInfoCmd.activeDate = null;
			getSubInfoCmd.resultCode=-1;
			getSubInfoCmd.resultString="Wrong ActiveDate field format:"+result.getActiveDate().getValue();
			logInfo(getSubInfoCmd.getRespString());
			queueResp.enqueue(getSubInfoCmd);
			return;
			
		}
		try{
			getSubInfoCmd.balance = result.getPpsBalance().isNil()?0:Integer.parseInt(result.getPpsBalance().getValue());
		}
		catch(NumberFormatException e){
			logError("OnGetSubInfoCmd: Error when parse balance field of msisdn "+getSubInfoCmd.msisdn);
			getSubInfoCmd.balance = 0;
			if(getSubInfoCmd.subType == GetSubInfoCmd.SUBS_TYPE_PREPAID){
				getSubInfoCmd.resultCode=-1;
				getSubInfoCmd.resultString="Wrong Balance field format:"+result.getActiveDate().getValue();
				logInfo(getSubInfoCmd.getRespString());
				queueResp.enqueue(getSubInfoCmd);
				return;
			}
		}
		
		getSubInfoCmd.subId = result.getSubID().isNil()?"":result.getSubID().getValue();
		if(getSubInfoCmd.subId == null|| getSubInfoCmd.subId.equals("")){
			getSubInfoCmd.resultCode=-1;
			getSubInfoCmd.resultString="SubID is NULL"+result.getActiveDate().getValue();
			logInfo(getSubInfoCmd.getRespString());
			queueResp.enqueue(getSubInfoCmd);
			return;
		}
		
		TopupPaymentApiWSQeuryProfileHeader header = result.getQeuryBasicInfoHeader().isNil()?null:result.getQeuryBasicInfoHeader().getValue();
		if(header!=null){
			getSubInfoCmd.resultCode=Integer.parseInt(header.getResultcode().getValue());
			getSubInfoCmd.resultString=header.getResultDes().getValue();
		}
		else{
			getSubInfoCmd.resultCode=-1;
			getSubInfoCmd.resultString="Call API function error";
		}
		
		logInfo(getSubInfoCmd.getRespString());
		queueResp.enqueue(getSubInfoCmd);
	}

	private void OnLoginCmd(LoginCmd loginCmd) {
		// TODO Auto-generated method stub
		logInfo(loginCmd.getReqString());
		
		TopupPaymentApiWS topupPaymentApiWS = new TopupPaymentApiWS();
		TopupPaymentApiWSPortType service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
		TopupPaymentApiWSLoginResult result = service.loginWS(loginCmd.spID, loginCmd.spPassword, ""+loginCmd.transactionId);
		loginCmd.result = PaymentGWResultCode.R_SUCCESS;
		loginCmd.resultCode=Integer.parseInt(result.getResultcode().getValue());
		loginCmd.resultString=result.getResultDescrib().getValue();
		loginCmd.token=result.getToken().getValue();
		logInfo(loginCmd.getRespString());
		queueResp.enqueue(loginCmd);
	}
	
	private void OnKeepAliveCmd(KeepAliveCmd keepAliveCmd) {
		// TODO Auto-generated method stub
		logInfo(keepAliveCmd.getReqString());
		TopupPaymentApiWS topupPaymentApiWS = new TopupPaymentApiWS();
		TopupPaymentApiWSPortType service = topupPaymentApiWS.getTopupPaymentApiWSHttpSoap11Endpoint();
		TopupPaymentApiWSKeepAliveResult result = service.keepalive(keepAliveCmd.token);
		keepAliveCmd.result = PaymentGWResultCode.R_SUCCESS;
		keepAliveCmd.resultCode=Integer.parseInt(result.getResultcode().getValue());
		keepAliveCmd.resultString=result.getResultDes().getValue();
		logInfo(keepAliveCmd.getRespString());
		queueResp.enqueue(keepAliveCmd);
	}
}
