/**
 * 
 */
package com.itpro.etopup.struct;

import java.sql.Timestamp;

/**
 * @author ducgv
 *
 */
public class RechargeCdrRecord {
	public final static int TYPE_RECHARGE = 0;
	public static final int TYPE_BATCH_RECHARGE = 1;
	
	public final static int STATUS_SUCCESS = 2;
	public final static int STATUS_FAILED = 3;
	
	public int payment_transaction_id;
	public Timestamp date_time;
	public int type;
	public String dealer_msisdn;
	public int dealer_id;
	public int balance_changed_amount;
	public long balance_before;
	public long balance_after;
	public String receiver_msidn;
	public int receiver_sub_type;
	public int recharge_value;
	public int transaction_id;
	public int result;
	public int result_code;
	public String result_description;
}
