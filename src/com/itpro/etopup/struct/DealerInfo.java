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
public class DealerInfo {
	//0-not active, 1-active, 2-block 30min, 3-block unlimited time, 4-deleted
	public static final int STATUS_NOT_ACTIVE = 0;
	public static final int STATUS_ACTIVE = 1;
	public static final int STATUS_BLOCK_WITH_TIME = 2;
	public static final int STATUS_BLOCK_UNLIMIT_TIME = 3;
	public static final int STATUS_DELETED = 4;
	
	public int id;
	public String msisdn;
	public String pin_code;
	public int parent_id;
	public Timestamp register_date;
	public String agent_init;
	public int agent_init_id;
	public String agent_approved;
	public int agent_approved_id;
	public String name;
	public Date birth_date;
	public String id_card_number;
	public int province_register;
	public String address;
	public long balance;
	public int active;
	public int category;
}
