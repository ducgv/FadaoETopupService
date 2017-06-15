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
	public static final int REQ_TYPE_RESET_PIN = 7;

	//-----------------------------------------------------------

	public static final int RC_REGISTER_DEALER_SUCCESS 			= 0;
	public static final int RC_REGISTER_SUB_DEALER_SUCCESS 		= 1;
	public static final int RC_ADD_BALANCE_SUCCESS 				= 2;
	public static final int RC_REFUND_ADD_BALANCE_SUCCESS 		= 3;
	public static final int RC_MOVE_DEALER_PROVINCE_SUCCESS 	= 4;
	public static final int RC_REFUND_MOVE_STOCK_SUCCESS 		= 5;
	public static final int RC_REFUND_RECHARGE_SUCCESS 			= 6;
	public static final int RC_REFUND_BATCH_RECHARGE_SUCCESS 	= 7;
	public static final int RC_DEALER_NOT_FOUND 				= 8;
	public static final int RC_AGENT_INIT_NOT_FOUND 			= 9;
	public static final int RC_AGENT_APPROVED_NOT_FOUND 		= 10;
	public static final int RC_DB_CONNECTION_ERROR 				= 11;
	public static final int RC_MOVE_DEALER_PROVINCE_FAILED 		= 12;
	public static final int RC_DEALER_IS_OUTSIDE_PROVINCE 		= 13;
	public static final int RC_DEALER_EXISTS 					= 14;
	public static final int RC_PARENT_DEALER_NOT_FOUND 			= 15;
	public static final int RC_TRANSACTION_NOT_FOUND 			= 16;
	public static final int RC_TRANSACTION_NOT_SUCCESS 			= 17;
	public static final int RC_TRANSACTION_REFUNDED 			= 18;
	public static final int RC_REFUND_AMOUNT_GREATER_THAN_TRANSACTION_AMOUNT	= 19;
	public static final int RC_REFUND_AMOUNT_GREATER_THAN_RECHARGE_AMOUNT 		= 20;
	public static final int RC_REFUND_TRANSACTION_TYPE_NOT_VALID 				= 21;
	public static final int RC_REFUND_BATCH_RECHARGE_SUBSCRIBER_NOT_FOUND 		= 22;
	public static final int RC_REFUND_MOVE_STOCK_RECEIVER_NOT_FOUND 			= 23;
	public static final int RC_REFUND_RECHARGE_CHARGING_FAILED 					= 24;
	public static final int RC_CALL_MOVE_STOCK_DB_FUNCTION_ERROR 				= 25;
	public static final int RC_REFUND_MOVE_STOCK_RECEIVER_BALANCE_NOT_ENOUGH 	= 26;
	public static final int RC_REFUND_RECHARGE_SUBSCRIBER_BALANCE_NOT_ENOUGH 	= 27;
	public static final int RC_REFUND_RECHARGE_GET_SUBSCRIBER_INFO_FAILED 		= 28;
	public static final int RC_REFUND_BATCH_RECHARGE_FAILED 					= 29;
	public static final int RC_ADD_BALANCE_ACCOUNT_IS_SUB_DEALER 				= 30;
	public static final int RC_RESET_PIN_SUCCESS 								= 31;
	
	public static String[] reqTypeString;
	public static String[] resultString;
	
	public static final int STATUS_SUCCESS = 2;
	public static final int STATUS_FAILED = 3;
	
	
	public static void init(){
		reqTypeString = new String[8];
		reqTypeString[REQ_TYPE_CREATE_DEALER] = "CreateDealer";
		reqTypeString[REQ_TYPE_ADD_BALANCE] = "AddBalance";
		reqTypeString[REQ_TYPE_REFUND] = "Refund";
		reqTypeString[REQ_TYPE_UNBLOCK] = "UnblockDealer";
		reqTypeString[REQ_TYPE_DEACTIVE_DEALER] = "DeactiveDealer";
		reqTypeString[REQ_TYPE_MOVE_DEALER_PROVINCE] = "MoveDealerProvince";
		reqTypeString[REQ_TYPE_CANCEL_ADD_BALANCE] = "CancelAddBalance";
		reqTypeString[REQ_TYPE_RESET_PIN] = "ResetPIN";

		//-----------------------------------------------------------
		
		resultString = new String[32];		
		resultString[RC_REGISTER_DEALER_SUCCESS] 		= "REGISTER_DEALER_SUCCESS";
		resultString[RC_REGISTER_SUB_DEALER_SUCCESS] 	= "REGISTER_SUB_DEALER_SUCCESS";
		resultString[RC_ADD_BALANCE_SUCCESS] 			= "ADD_BALANCE_SUCCESS";
		resultString[RC_REFUND_ADD_BALANCE_SUCCESS] 	= "REFUND_ADD_BALANCE_SUCCESS";
		resultString[RC_MOVE_DEALER_PROVINCE_SUCCESS] 	= "MOVE_DEALER_PROVINCE_SUCCESS";
		resultString[RC_REFUND_MOVE_STOCK_SUCCESS] 		= "REFUND_MOVE_STOCK_SUCCESS";
		resultString[RC_REFUND_RECHARGE_SUCCESS] 		= "REFUND_RECHARGE_SUCCESS";
		resultString[RC_REFUND_BATCH_RECHARGE_SUCCESS] 	= "REFUND_BATCH_RECHARGE_SUCCESS";
		resultString[RC_DEALER_NOT_FOUND] 				= "DEALER_NOT_FOUND";
		resultString[RC_AGENT_INIT_NOT_FOUND] 			= "AGENT_INIT_NOT_FOUND";
		resultString[RC_AGENT_APPROVED_NOT_FOUND] 		= "AGENT_APPROVED_NOT_FOUND";
		resultString[RC_DB_CONNECTION_ERROR] 			= "DB_CONNECTION_ERROR";
		resultString[RC_MOVE_DEALER_PROVINCE_FAILED] 	= "MOVE_DEALER_PROVINCE_FAILED";
		resultString[RC_DEALER_IS_OUTSIDE_PROVINCE]		= "DEALER_IS_OUTSIDE_PROVINCE";
		resultString[RC_DEALER_EXISTS] 					= "DEALER_EXISTS";
		resultString[RC_PARENT_DEALER_NOT_FOUND] 		= "PARENT_DEALER_NOT_FOUND";
		resultString[RC_TRANSACTION_NOT_FOUND] 			= "TRANSACTION_NOT_FOUND";
		resultString[RC_TRANSACTION_NOT_SUCCESS] 		= "TRANSACTION_NOT_SUCCESS";
		resultString[RC_TRANSACTION_REFUNDED] 			= "TRANSACTION_REFUNDED";
		resultString[RC_REFUND_AMOUNT_GREATER_THAN_TRANSACTION_AMOUNT] 	= "REFUND_AMOUNT_GREATER_THAN_TRANSACTION_AMOUNT";
		resultString[RC_REFUND_AMOUNT_GREATER_THAN_RECHARGE_AMOUNT] 	= "REFUND_AMOUNT_GREATER_THAN_RECHARGE_AMOUNT";
		resultString[RC_REFUND_TRANSACTION_TYPE_NOT_VALID] 				= "REFUND_TRANSACTION_TYPE_NOT_VALID";
		resultString[RC_REFUND_BATCH_RECHARGE_SUBSCRIBER_NOT_FOUND] 	= "REFUND_BATCH_RECHARGE_SUBSCRIBER_NOT_FOUND";
		resultString[RC_REFUND_MOVE_STOCK_RECEIVER_NOT_FOUND] 			= "REFUND_MOVE_STOCK_RECEIVER_NOT_FOUND";
		resultString[RC_REFUND_RECHARGE_CHARGING_FAILED] 				= "REFUND_RECHARGE_CHARGING_FAILED";
		resultString[RC_CALL_MOVE_STOCK_DB_FUNCTION_ERROR] 				= "CALL_MOVE_STOCK_DB_FUNCTION_ERROR";
		resultString[RC_REFUND_MOVE_STOCK_RECEIVER_BALANCE_NOT_ENOUGH] 	= "REFUND_MOVE_STOCK_RECEIVER_BALANCE_NOT_ENOUGH";
		resultString[RC_REFUND_RECHARGE_SUBSCRIBER_BALANCE_NOT_ENOUGH] 	= "REFUND_RECHARGE_SUBSCRIBER_BALANCE_NOT_ENOUGH";
		resultString[RC_REFUND_RECHARGE_GET_SUBSCRIBER_INFO_FAILED] 	= "REFUND_RECHARGE_GET_SUBSCRIBER_INFO_FAILED";
		resultString[RC_REFUND_BATCH_RECHARGE_FAILED] 		= "REFUND_BATCH_RECHARGE_FAILED";
		resultString[RC_ADD_BALANCE_ACCOUNT_IS_SUB_DEALER] 	= "ADD_BALANCE_ACCOUNT_IS_SUB_DEALER";
		resultString[RC_RESET_PIN_SUCCESS] 					= "RESET_PIN_SUCCESS";
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
	public int result_code;
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
		case REQ_TYPE_RESET_PIN:
			return "AgentReq: reqType:"+reqTypeString[req_type]+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn;
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
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		case REQ_TYPE_ADD_BALANCE:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; cash_value:"+addBalanceInfo.cash_value+
					"; commision_value:"+addBalanceInfo.commision_value+
					"; balance_add_amount:"+balance_add_amount+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		case REQ_TYPE_REFUND:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; refundMsisdn:"+refund_msisdn+
					"; refundAmount:"+refund_amount+
					"; transactionId:"+refund_transaction_id+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		case REQ_TYPE_UNBLOCK:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		case REQ_TYPE_DEACTIVE_DEALER:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; result:"+resultString[result_code];
		case REQ_TYPE_MOVE_DEALER_PROVINCE:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; dest_province_code:"+dest_province_code+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		case REQ_TYPE_CANCEL_ADD_BALANCE:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; transactionId:"+refund_transaction_id+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		case REQ_TYPE_RESET_PIN:
			return "AgentReqResult: reqType:"+reqTypeString[req_type]+
					"; agentUserName:"+agent_username+
					"; agentId:"+agent_id+
					"; msisdn:"+dealer_msisdn+
					"; status:"+status+
					"; resultCode:"+result_code+
					"; resultString:"+resultString[result_code];
		default:
			return "AgentReqResult: reqType:unknown"+
			"; agentUserName:"+agent_username+
			"; agentId:"+agent_id;
			
		}
	}
	
}
