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
	public int id;
	public String msisdn;
	public String pin_code;
	public Timestamp register_date;
	public String agent_approved;
	public int agent_approved_id;
	public String name;
	public Date birth_date;
	public String id_card_number;
	public String province;
	public String address;
	public long balance;
	public int active;
}
