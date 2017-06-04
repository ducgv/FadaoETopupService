/**
 * 
 */
package com.itpro.etopup.struct.dealercmd;

import com.itpro.etopup.struct.DealerRequest;

/**
 * @author ducgv
 *
 */
public class ChangePinCmd extends RequestCmd {
	public String newPin;
	public ChangePinCmd() {
		// TODO Auto-generated constructor stub
		cmdType = DealerRequest.CMD_TYPE_CHANGE_PIN;
	}
	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.dealercmd.RequestCmd#getReqString()
	 */
	@Override
	public String getReqString() {
		// TODO Auto-generated method stub
		return "ChangePinReq: msisdn:"+msisdn +
				"; oldPIN:"+pinCode +
				"; newPIN:"+newPin;
	}

	/* (non-Javadoc)
	 * @see com.itpro.etopup.struct.dealercmd.RequestCmd#getRespString()
	 */
	@Override
	public String getRespString() {
		// TODO Auto-generated method stub
		return "ChangePinResp: msisdn:"+msisdn +
				"; resultCode:"+resultCode +
				"; resultString:"+resultString;
	}

}
