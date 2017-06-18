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
	public final static int TRANS_TYPE_ADD_DEALER = 0;
	public final static int TRANS_TYPE_STOCK_ALLOCATION = 1;
	public final static int TRANS_TYPE_RECHARGE_VOUCHER = 2;
	public final static int TRANS_TYPE_BATCH_RECHARGE = 3;
	public final static int TRANS_TYPE_STOCK_MOVE_OUT = 4;
	public final static int TRANS_TYPE_CANCEL_RECHARGE_VOUCHER = 5;
	public final static int TRANS_TYPE_CANCEL_STOCK_MOVE_OUT = 6;
	public final static int TRANS_TYPE_CANCEL_STOCK_ALLOCATION = 7;
	public final static int TRANS_TYPE_ADD_RETAILER = 8;
	public final static int TRANS_TYPE_MOVE_DEALER_PROVINCE_SOURCE = 9;
	public final static int TRANS_TYPE_MOVE_DEALER_PROVINCE_DESTINATION = 10;
	public final static int TRANS_TYPE_STOCK_MOVE_IN = 11;
	public final static int TRANS_TYPE_CANCEL_STOCK_MOVE_IN = 12;
	public final static int TRANS_TYPE_STOCK_ALLOCATION_FREE = 13;
	
	public final static int TRANS_STATUS_SUCCESS = 2;
	public final static int TRANS_STATUS_FAILED = 3;

    public final static int TRANS_NOT_REFUNDED_STATUS = 0;	
	public final static int TRANS_REFUNDED_STATUS = 1;

	public int id;
	public int type;
	public Timestamp date_time;
	public String dealer_msisdn;
	public int dealer_id;
	public int dealer_parent_id;
	public int dealer_province;
	public int customer_care;
	public int dealer_category;
	public int parent_id;
	public String agent;
	public int agent_id;
	public String approved;
	public int approved_id;
	public int dealer_new_id;
	public int dealer_new_province;
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
	public int refer_transaction_id;
	public int refund_status;
	public int batch_recharge_id;
	public int status;
	public String result_description;
	public int batch_recharge_succes;
	public int batch_recharge_fail;
	public String remark;
	public AddBalanceInfo addBalanceInfo;
}
