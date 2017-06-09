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
	
	public int refund_status;
	public int refund_result_code;
	public String refund_result_string;
	
    public String toString(){
        return "BatchRechargeElement: id:"+id+
                "; dealer_id:"+dealer_id+
                "; dealer_msisdn:"+dealer_msisdn+
                "; recharge_msisdn:"+recharge_msisdn+
                "; recharge_value:"+recharge_value+
                "; status:"+status+
                "; result_code:"+result_code+
                "; result_string:"+result_string+
                "; batch_recharge_id:"+batch_recharge_id+
                "; refund_status:"+refund_status+
                "; refund_result_code:"+refund_result_code+
                "; refund_result_string:"+refund_result_string;
    }
}
