/**
 * 
 */
package com.itpro.etopup.struct;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author ducgv
 *
 */
public class AgentRequest {
	//0-create dealer, 1-add_balance, 2-refund
	public static final int REQ_TYPE_CREATE_DEALER = 0;
	public static final int REQ_TYPE_ADD_BALANCE = 1;
	public static final int REQ_TYPE_REFUND = 2;
	
	public static final int STATUS_SUCCESS = 2;
	public static final int STATUS_FAILED = 3;
	
	public int id;
	public int req_type;
	public Timestamp req_date;
	public String agent_username;
	public int agent_id;
	public int agent_province;
	public String dealer_msisdn;
	public int dealer_id;
	public String dealer_name;
	public String dealer_id_card_number;
	public Date dealer_birthdate;
	public int dealer_province;
	public String dealer_address;
	public long balance_add_amount;
	public long cash_value;
	public String invoice_code;
	public int refund_transaction_id;
	public int status;
	public String result_description;
	public int transaction_id;
	public String web_password;
	public String getRespString() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
