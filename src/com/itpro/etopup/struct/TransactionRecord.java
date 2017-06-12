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
	public final static int TRANS_TYPE_CREATE_DEALER = 0;
	public final static int TRANS_TYPE_ADD_BALANCE = 1;
	public final static int TRANS_TYPE_RECHARGE = 2;
	public final static int TRANS_TYPE_BATCH_RECHARGE = 3;
	public final static int TRANS_TYPE_MOVE_STOCK = 4;
	public final static int TRANS_TYPE_REFUND_RECHARGE = 5;
	public final static int TRANS_TYPE_REFUND_MOVE_STOCK = 6;
	public final static int TRANS_TYPE_REFUND_ADD_BALANCE = 7;
	public final static int TRANS_TYPE_CREATE_SUB_DEALER = 8;
	
	public final static int TRANS_STATUS_SUCCESS = 2;
	public final static int TRANS_STATUS_FAILED = 3;

    public final static int TRANS_NOT_REFUNDED_STATUS = 0;	
	public final static int TRANS_REFUNDED_STATUS = 1;

	public int id;
	public int type;
	public Timestamp date_time;
	public String dealer_msisdn;
	public int dealer_id;
	public int dealer_province;
	public int parent_id;
	public String agent;
	public int agent_id;
	public String approved;
	public int approved_id;
	public String partner_msisdn;
	public int partner_id;
	public long partner_balance_before;
	public long partner_balance_after;
	public String recharge_msidn;
	public int recharge_sub_type;
	public int recharge_value;
	public long balance_before;
	public long transaction_amount_req;
	public long balance_changed_amount = 0;
	public long balance_after;
	public long cash_value;
	public String invoice_code;
	public int refund_transaction_id;
	public int refund_status;
	public int batch_recharge_id;
	public int status;
	public String result_description;
	public int batch_recharge_succes;
	public int batch_recharge_fail;
}
