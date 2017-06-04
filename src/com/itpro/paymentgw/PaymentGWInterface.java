/**
 * 
 */
package com.itpro.paymentgw;

import java.util.Hashtable;
import com.itpro.paymentgw.cmd.PaymentGWCmd;
import com.itpro.util.ProcessingThread;
import com.itpro.util.Queue;

/**
 * @author Giap Van Duc
 *
 */
public class PaymentGWInterface extends ProcessingThread {	
	private static final int MIN_SEQ = 0x00000001;
	private static final int MAX_SEQ = 0x0fffffff;
	private int curSeq;
	private int getSeq(){
		if(curSeq>MAX_SEQ){
			curSeq = MIN_SEQ;
		}
		return curSeq++;
	}
	
	Hashtable<Integer, PaymentGWCmd> userCmdProcessingList = new Hashtable<Integer, PaymentGWCmd>();
	
//	private AccountServiceLocator accountServiceLocator;
	
	public Queue queueUserRequest = new Queue();
	private Queue queueResp = new Queue();
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
		setHeartBeatInterval(5000);
		setLogPrefix("[PaymentGWInterface] ");
		PaymentGWResultCode.init();
	}

	
	
	/* (non-Javadoc)
	 * @see com.itpro.util.ProcessingThread#process()
	 */
	@Override
	protected void process() {
		// TODO Auto-generated method stub	
		PaymentGWCmd paymentGWCmd = (PaymentGWCmd)queueUserRequest.dequeue();
		if(paymentGWCmd!=null){
			paymentGWCmd.seq = getSeq();
			logInfo(paymentGWCmd.getReqString());			
			PaymentGWSession paymentGWSession = new PaymentGWSession(paymentGWCmd, queueResp);
			paymentGWSession.setLogger(logger);
			userCmdProcessingList.put(paymentGWCmd.seq, paymentGWCmd);
			paymentGWSession.start();

		}

		PaymentGWCmd userCmdResp = (PaymentGWCmd)queueResp.dequeue();
		if(userCmdResp!=null){
			userCmdProcessingList.remove(userCmdResp.seq);
			logInfo(userCmdResp.getRespString());
			userCmdResp.queueResp.enqueue(userCmdResp);		

		}
	}
}
