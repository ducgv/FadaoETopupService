package com.itpro.etopup.struct.dealercmd;

import java.sql.Timestamp;

public class BatchRechargeElement {
	public static final int STATUS_SUCCESS = 2;
	public static final int STATUS_FAILED = 3;
	public int id;
	public int dealer_id;
	public String dealer_msisdn;
	public String recharge_msisdn;
	public int recharge_value;
	public Timestamp datetime;
	public int db_return_code;
	public int balanceBefore;
	public int balanceAfter;
	public int status;
	public int result_code;
	public String result_string;
	public int batch_recharge_id;
}
