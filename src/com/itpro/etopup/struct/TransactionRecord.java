/**
 * 
 */
package com.itpro.etopup.struct;

import java.sql.Timestamp;

/**
 * @author ducgv
 *
 */
public class TransactionRecord {
	public final static int TRANS_TYPE_CREATE_ACCOUNT = 0;
	public final static int TRANS_TYPE_ADD_BALANCE = 1;
	public final static int TRANS_TYPE_RECHARGE = 2;
	public final static int TRANS_TYPE_BATCH_RECHARGE = 3;
	public final static int TRANS_TYPE_MOVE_STOCK = 4;
	public final static int TRANS_TYPE_REFUND_RECHARGE = 5;
	public final static int TRANS_TYPE_REFUND_MOVE_STOCK = 6;
	
	public final static int TRANS_STATUS_SUCCESS = 2;
	public final static int TRANS_STATUS_FAILED = 3;
	
	public int id;
	public int type;
	public Timestamp date_time;
	public String dealer_msisdn;
	public int dealer_id;
	public String agent;
	public int agent_id;
	public String partner_msisdn;
	public int partner_id;
	public long partner_balance_before;
	public long partner_balance_after;
	public String recharge_msidn;
	public int recharge_sub_type;
	public int recharge_value;
	public long balance_before;
	public long balance_changed_amount;
	public long balance_after;
	public long cash_value;
	public String invoice_code;
	public int refund_transaction_id;
	public int batch_recharge_id;
	public int status;
	public String result_description;
}
