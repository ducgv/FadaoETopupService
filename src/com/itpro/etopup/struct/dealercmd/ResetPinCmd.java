/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import com.itpro.etopup.struct.DealerRequest;

/**
 * @author ducgv
 *
 */
public class ResetPinCmd extends RequestCmd {
	public String newPin = "";
	public ResetPinCmd() {
		// TODO Auto-generated constructor stub
		cmdType = DealerRequest.CMD_TYPE_RESET_PIN;
	}
	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.dealercmd.RequestCmd#getReqString()
	 */
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "ResetPinReq: msisdn:"+msisdn +
				"; oldPIN:"+pinCode +
				"; newPIN:"+newPin;
	}

	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.dealercmd.RequestCmd#getRespString()
	 */
	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "ResetPinResp: msisdn:"+msisdn +
				"; resultCode:"+resultCode +
				"; resultString:"+resultString;
	}

}
