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
public class AgentRequest {
	//0-create dealer, 1-add_balance, 2-refund
	//0-create dealer, 1-add_balance, 2-refund, 3-Unblock, 4-Deactive
	public static final int REQ_TYPE_CREATE_DEALER = 0;
	public static final int REQ_TYPE_ADD_BALANCE = 1;
	public static final int REQ_TYPE_REFUND = 2;
	public static final int REQ_TYPE_UNBLOCK = 3;
	public static final int REQ_TYPE_DEACTIVE_DEALER = 4;
	public static final int REQ_TYPE_MOVE_DEALER_PROVINCE = 5;
	public static final int REQ_TYPE_CANCEL_ADD_BALANCE = 6;
	
	
	public static String[] reqTypeString;
	
	public static final int STATUS_SUCCESS = 2;
	public static final int STATUS_FAILED = 3;
	
	public static void init(){
		reqTypeString = new String[7];
		reqTypeString[REQ_TYPE_CREATE_DEALER] = "CreateDealer";
		reqTypeString[REQ_TYPE_ADD_BALANCE] = "AddBalance";
		reqTypeString[REQ_TYPE_REFUND] = "Refund";
		reqTypeString[REQ_TYPE_UNBLOCK] = "UnblockDealer";
		reqTypeString[REQ_TYPE_DEACTIVE_DEALER] = "DeactiveDealer";
		reqTypeString[REQ_TYPE_MOVE_DEALER_PROVINCE] = "MoveDealerProvince";
		reqTypeString[REQ_TYPE_CANCEL_ADD_BALANCE] = "CancelAddBalance";
	}
	
	public int id;
	public int req_type;
	public Timestamp req_date;
	public String agent_username;
	public int agent_id;
	public String agent_approved_username;
	public int agent_approved_id;
	public int option_dealer_province_code;
	public String dealer_msisdn;
	public int dealer_id;
	public String dealer_name;
	public int dealer_parent_id;
	public String dealer_id_card_number;
	public Date dealer_birthdate;
	public int dealer_province;
	public String dealer_address;
	public long balance_add_amount;
	public int dest_province_code;
	public int refund_transaction_id;
	public String refund_msisdn;
	public int refund_amount;
	public int status;
	public String result_description;
	public int transaction_id;
	public String web_password;
	public int category;
	public AddBalanceInfo addBalanceInfo = new AddBalanceInfo();
	public String getReqString() {
		// TODO Auto-generated method stub
		switch(req_type){
		case REQ_TYPE_CREATE_DEALER:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn+
					(dealer_parent_id!=0?"; parent_id:"+dealer_parent_id:"")+
					"; categoryId:"+category;
		case REQ_TYPE_ADD_BALANCE:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn+
					"; cash_value:"+addBalanceInfo.cash_value+
					"; commision_value:"+addBalanceInfo.commision_value;
		case REQ_TYPE_REFUND:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn+
					"; refundMsisdn:"+refund_msisdn+
					"; refundAmount:"+refund_amount+
					"; transactionId:"+refund_transaction_id;
		case REQ_TYPE_UNBLOCK:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn;
		case REQ_TYPE_DEACTIVE_DEALER:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn;
		case REQ_TYPE_MOVE_DEALER_PROVINCE:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn+
					"; dest_province_code:"+dest_province_code;
		case REQ_TYPE_CANCEL_ADD_BALANCE:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; agentApprovedId:"+agent_approved_id+
					"; msisdn:"+dealer_msisdn+
					"; transactionId:"+refund_transaction_id;
		default:
			return "AgentReq: reqType:unknown"+
			"; agentId:"+agent_id+
			"; agentApprovedId:"+agent_approved_id;
		}
	}
	
	public String getRespString() {
		// TODO Auto-generated method stub
		switch(req_type){
		case REQ_TYPE_CREATE_DEALER:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					(dealer_parent_id!=0?"; parent_id:"+dealer_parent_id:"")+
					"; status:"+status+
					"; result:"+result_description;
		case REQ_TYPE_ADD_BALANCE:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; cash_value:"+addBalanceInfo.cash_value+
					"; commision_value:"+addBalanceInfo.commision_value+
					"; balance_add_amount:"+balance_add_amount+
					"; status:"+status+
					"; result:"+result_description;
		case REQ_TYPE_REFUND:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; refundMsisdn:"+refund_msisdn+
					"; refundAmount:"+refund_amount+
					"; transactionId:"+refund_transaction_id+
					"; status:"+status+
					"; result:"+result_description;
		case REQ_TYPE_UNBLOCK:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; status:"+status+
					"; result:"+result_description;
		case REQ_TYPE_DEACTIVE_DEALER:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; status:"+status+
					"; result:"+result_description;
		case REQ_TYPE_MOVE_DEALER_PROVINCE:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; dest_province_code:"+dest_province_code+
					"; status:"+status+
					"; result:"+result_description;
		case REQ_TYPE_CANCEL_ADD_BALANCE:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; transactionId:"+refund_transaction_id+
					"; status:"+status+
					"; result:"+result_description;
		default:
			return "AgentReqResult: reqType:unknown"+
			"; agentUserName:"+agent_username+
			"; agentId:"+agent_id;
			
		}
	}
	
}
