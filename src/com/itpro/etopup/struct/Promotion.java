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
public class Promotion {
	public static final int PROMOTION_TYPE_BALANCE = 0;
	public static final int PROMOTION_TYPE_DATA = 1;
	public static final int PARAM_TYPE_FIX = 0;
	public static final int PARAM_TYPE_REFER = 1;
	public static final int STATUS_ACTIVE = 1;
	public int id;
	public Timestamp date_time;
	public int agent_init_id;
	public Date from_date;
	public Date to_date;
	public int promotion_type;
	public int topup_value_level;
	public int param_type;
	public double param_value;
}
