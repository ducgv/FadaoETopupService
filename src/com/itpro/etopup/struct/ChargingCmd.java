/**
 * 
 */
package com.itpro.etopup.struct;
/**
 * @author ducgv
 *
 */
import java.sql.Timestamp;

public class ChargingCmd {
	public final static int RESULT_OK = 405000000;
	public String  msisdn;
	public String recharge_msidn;
	public Timestamp charge_date;
	public long chargeValue;
	public int resultCode;
	public String resultString;
	public int transactionID;
	public String spID;
	public String serviceID;
	public ChargingCmd(){
		//chargeValue = offerRecord.package_value+offerRecord.package_service_fee;
	}
	
	public String toString(){
		String str = "ChargingCmd:"
				+ " msisdn:"+msisdn
				+ " recharge_msidn:"+recharge_msidn
				+ "; chargeValue:"+chargeValue
				+ "; resultCode:"+resultCode
				+ "; resultString:"+resultString
		        + "; transactionID:"+transactionID
		        + "; spID:"+spID
		        + "; serviceID:"+serviceID;
		return str;
	}
}
