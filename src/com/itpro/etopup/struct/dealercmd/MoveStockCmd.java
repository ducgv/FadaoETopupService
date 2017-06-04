/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;

/**
 * @author ducgv
 *
 */
public class MoveStockCmd extends RequestCmd {
	public DealerInfo receiverInfo;
	public String receiverMsisdn;
	public int amount;
	public int db_return_code;
	public int balanceAfter;
	public int receiverBalanceAfter;
	public MoveStockCmd() {
		// TODO Auto-generated constructor stub
		cmdType = DealerRequest.CMD_TYPE_MOVE_STOCK;
	}
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "MoveStockReq: msisdn:"+msisdn+
				"; receiver:"+receiverMsisdn+
				"; amount:"+amount;
	}

	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "MoveStockResp: msisdn:"+msisdn+
				"; receiver:"+receiverMsisdn+
				"; amount:"+amount+
				"; balanceAfter:"+balanceAfter+
				"; receiverBalanceAfter:"+receiverBalanceAfter+
				"; resultCode:"+resultCode+
				"; resultString:"+resultString;
	}

}
