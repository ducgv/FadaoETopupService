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
public class RechargeCdrRecord {
	public final static int TYPE_RECHARGE = 0;
	public static final int TYPE_BATCH_RECHARGE = 1;
	
	public final static int STATUS_SUCCESS = 2;
	public final static int STATUS_FAILED = 3;
	
	public int payment_transaction_id;
	public Timestamp date_time;
	public int type; //0-recharge, 1-batchRecharge
	public String dealer_msisdn;
	public int dealer_id;
	public int dealer_province;
	public int dealer_category;
	public int balance_changed_amount;
	public long balance_before;
	public long balance_after;
	public String receiver_msidn;
	public int receiver_province;
	public int receiver_sub_type;
	public Date receiver_active_date;
	public Date receiver_new_expire_date;
	public int recharge_value;
	public int transaction_id;
	public int bonus_balance;
	public int bonus_data;
	public int promotion_balance_id;
	public int promotion_data_id;
	public int result;
	public int result_code;
	public String result_description;
}
