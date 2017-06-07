/**
 * 
 */
package com.itpro.etopup.struct;

import java.sql.Timestamp;

/**
 * @author ducgv
 *
 */
public class CDRRecord {
	public static final int ADD_BALANCE_SUCCESS = 2;
	public static final int ADD_BALANCE_FAILED = 3;
	public Timestamp date_time;
	public String msisdn;
	public int province_code;
	public String sub_id;
	public int offer_id;
	public int offer_type;
	public String package_name;
	public int package_value;
	public int service_fee;
	public int add_amount;
	public int result_code;
	public String result_string;
	public int status;
	public int transactionID;
	public String spID;
	public String token;
	public String toString(){
		String str= "CDRRecord: "
				+ "msisdn:"+msisdn
				+ "; province_code:"+province_code
				+ "; sub_id:"+sub_id
				+ "; date_time:"+date_time
				+ "; offer_id:"+offer_id
				+ "; offer_type:"+offer_type
				+ "; package_name:"+package_name
				+ "; package_value:"+package_value
				+ "; service_fee:"+service_fee
				+ "; add_amount:"+add_amount
				+ "; result_code:"+result_code
				+ "; result_string:"+result_string
				+ "; status:"+status
				+ "; transactionID:"+transactionID
				+ "; spID:"+spID
				+ "; token:"+token;
		return str;
		
	}
}
