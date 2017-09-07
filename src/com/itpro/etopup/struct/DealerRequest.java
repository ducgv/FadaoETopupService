/**
 * 
 */
package com.itpro.etopup.struct;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import com.itpro.etopup.struct.dealercmd.BatchRechargeCmd;
import com.itpro.etopup.struct.dealercmd.ChangePinCmd;
import com.itpro.etopup.struct.dealercmd.MoveStockCmd;
import com.itpro.etopup.struct.dealercmd.QueryBalanceCmd;
import com.itpro.etopup.struct.dealercmd.RechargeCmd;
import com.itpro.etopup.struct.dealercmd.RequestCmd;

/**
 * @author Giap Van Duc
 *
 */
public class DealerRequest {
	public static final int CMD_TYPE_QUERY_BALANCE = 0;
	public static final int CMD_TYPE_CHANGE_PIN = 1;
	public static final int CMD_TYPE_RECHARGE = 2;
	public static final int CMD_TYPE_MOVE_STOCK = 3;
	public static final int CMD_TYPE_BATCH_RECHARGE = 4;
	public static final int CMD_TYPE_WRONG_SYNTAX = 5;
	
	public int id;
	public String msisdn;
	public String req_src;
	public Timestamp req_date;
	public String[] cmd_params;
	public String content;
	public int status;
	public String result;
	public int dealer_id = 0;
	public int transaction_id = 0;
	public int lang;
	public RequestCmd requestCmd;
	public int cmd_type;
	
	public String toString(){
		String result = "DealerRequest: ";
		result += "misdn:"+msisdn;
		result += "; req_src:"+req_src;
		result += "; content:"+content;
		return result;
	}
	
	public boolean containsEmptyElement(String[] params){
		List<String> list = Arrays.asList(params);
		return list.contains("")||list.contains(null);
	}
	
	public void analizeContent() {
		String str = content;
		while(str.charAt(0)=='*')
			str = str.replaceFirst("[*]", "");
		cmd_params = str.replaceAll("#", "").trim().split("[*]", 7);

		// TODO Auto-generated method stub
		switch(cmd_params.length){
		case 1:
			if(!containsEmptyElement(cmd_params)){
				//new
				cmd_type = CMD_TYPE_QUERY_BALANCE;
				QueryBalanceCmd queryBalanceCmd1 = new QueryBalanceCmd();
				queryBalanceCmd1.msisdn = msisdn;
				queryBalanceCmd1.pinCode = null;
				requestCmd = queryBalanceCmd1;
			}
			else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		case 2:
			if(!containsEmptyElement(cmd_params)){
				if(req_src.equalsIgnoreCase("WEB")){
					cmd_type = CMD_TYPE_BATCH_RECHARGE;
					BatchRechargeCmd batchRechargeCmd = new BatchRechargeCmd();
					batchRechargeCmd.msisdn = msisdn;
					batchRechargeCmd.batch_recharge_id = Integer.parseInt(cmd_params[1]);
					requestCmd=batchRechargeCmd;
				}
				else{
					cmd_type = CMD_TYPE_RECHARGE;
					RechargeCmd rechargeToOwnerCmd2 = new RechargeCmd();
					rechargeToOwnerCmd2.msisdn = msisdn;
					rechargeToOwnerCmd2.checkPin = false;
					rechargeToOwnerCmd2.rechargeMsisdn = msisdn.replaceFirst("856", "");
					try {
						rechargeToOwnerCmd2.amount = Integer.parseInt(cmd_params[1]);
						requestCmd = rechargeToOwnerCmd2;
					} catch (NumberFormatException e) {
						// TODO: handle exception
						cmd_type = CMD_TYPE_WRONG_SYNTAX;
					}
					/*
				if(rechargeToOwnerCmd2.amount<1000)
					rechargeToOwnerCmd2.amount = rechargeToOwnerCmd2.amount*1000;
					 */
					
				}
			}
			else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		case 3:
			if(!containsEmptyElement(cmd_params)){
				//new
				cmd_type = CMD_TYPE_RECHARGE;
				RechargeCmd rechargeToOwnerCmd3 = new RechargeCmd();
				rechargeToOwnerCmd3.msisdn = msisdn;
				rechargeToOwnerCmd3.rechargeMsisdn = msisdn.replaceFirst("856", "");
				rechargeToOwnerCmd3.pinCode = cmd_params[1];
				try {
					rechargeToOwnerCmd3.amount = Integer.parseInt(cmd_params[2]);
					/*
					if(rechargeToOwnerCmd3.amount<1000)
						rechargeToOwnerCmd3.amount = rechargeToOwnerCmd3.amount*1000;
					 */
					requestCmd = rechargeToOwnerCmd3;
				} catch (NumberFormatException e) {
					// TODO: handle exception
					cmd_type = CMD_TYPE_WRONG_SYNTAX;
				}			
			}
			else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		case 4:
			if(!containsEmptyElement(cmd_params)){
				//new
				if(cmd_params[2].equals("2")&&cmd_params[3].equals("2")){
					cmd_type = CMD_TYPE_QUERY_BALANCE;
					QueryBalanceCmd queryBalanceCmd4 = new QueryBalanceCmd();
					queryBalanceCmd4.msisdn = msisdn;
					queryBalanceCmd4.pinCode = cmd_params[1];
					requestCmd = queryBalanceCmd4;
				}
				//old
				else if(cmd_params[2].equals("3")){
					cmd_type = CMD_TYPE_CHANGE_PIN;
					ChangePinCmd changePinCmd = new ChangePinCmd();
					changePinCmd.msisdn = msisdn;
					changePinCmd.pinCode = cmd_params[1];
					changePinCmd.newPin = cmd_params[3];
					requestCmd = changePinCmd;
				}
				//new
				else if(cmd_params[1].equals("0")){
					cmd_type = CMD_TYPE_CHANGE_PIN;
					ChangePinCmd changePinCmd = new ChangePinCmd();
					changePinCmd.msisdn = msisdn;
					changePinCmd.pinCode = cmd_params[2];
					changePinCmd.newPin = cmd_params[3];
					requestCmd = changePinCmd;
				}
				//new
				else {
					cmd_type = CMD_TYPE_RECHARGE;
					RechargeCmd rechargeToOtherCmd4 = new RechargeCmd();
					rechargeToOtherCmd4.msisdn = msisdn;
					rechargeToOtherCmd4.pinCode = cmd_params[1];
					rechargeToOtherCmd4.rechargeMsisdn = cmd_params[2];
					try {
						rechargeToOtherCmd4.amount = Integer.parseInt(cmd_params[3]);
						/*
						if(rechargeToOtherCmd4.amount<=999)
							rechargeToOtherCmd4.amount = rechargeToOtherCmd4.amount*1000;
						 */
						requestCmd = rechargeToOtherCmd4;
					} catch (NumberFormatException e) {
						// TODO: handle exception
						cmd_type = CMD_TYPE_WRONG_SYNTAX;
					}				
				}
			}
			else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		case 5:
			if(!containsEmptyElement(cmd_params)){
				if(cmd_params[1].equals("0")){
					//new
					cmd_type = CMD_TYPE_MOVE_STOCK;
					MoveStockCmd moveStockCmd5 = new MoveStockCmd();
					moveStockCmd5.msisdn = msisdn;
					moveStockCmd5.pinCode = cmd_params[2];
					moveStockCmd5.receiverMsisdn = cmd_params[3];
					try {
						moveStockCmd5.amount = Integer.parseInt(cmd_params[4]);
						/*
						if(moveStockCmd5.amount<=999)
							moveStockCmd5.amount = moveStockCmd5.amount*1000;
						 */
						requestCmd = moveStockCmd5;
					} catch (NumberFormatException e) {
						// TODO: handle exception
						cmd_type = CMD_TYPE_WRONG_SYNTAX;
					}
				}
				else{
					cmd_type = CMD_TYPE_WRONG_SYNTAX;
				}
			}
			else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		case 6:
			if(!containsEmptyElement(cmd_params)){
				if(cmd_params[2].equals("1")&&cmd_params[5].equals("1")){
					//old
					cmd_type = CMD_TYPE_RECHARGE;
					RechargeCmd rechargeToOwnerCmd6 = new RechargeCmd();
					rechargeToOwnerCmd6.msisdn = msisdn;
					rechargeToOwnerCmd6.pinCode = cmd_params[1];
					rechargeToOwnerCmd6.rechargeMsisdn = cmd_params[3];
					try {
						rechargeToOwnerCmd6.amount = Integer.parseInt(cmd_params[4]);
						/*
						if(rechargeToOwnerCmd6.amount<=999)
							rechargeToOwnerCmd6.amount = rechargeToOwnerCmd6.amount*1000;
						 */
						requestCmd = rechargeToOwnerCmd6;
					} catch (NumberFormatException e) {
						// TODO: handle exception
						cmd_type = CMD_TYPE_WRONG_SYNTAX;
					}
					
				}
				else{
					cmd_type = CMD_TYPE_WRONG_SYNTAX;
				}
			}else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		case 7:
			if(cmd_params[2].equals("1")&&cmd_params[5].equals("1")&&cmd_params[6].equals("")&&!cmd_params[4].equals("")){
				//old
				cmd_type = CMD_TYPE_RECHARGE;
				RechargeCmd rechargeToOtherCmd7 = new RechargeCmd();
				rechargeToOtherCmd7.msisdn = msisdn;
				rechargeToOtherCmd7.pinCode = cmd_params[1];
				rechargeToOtherCmd7.rechargeMsisdn = cmd_params[3];
				try {
					rechargeToOtherCmd7.amount = Integer.parseInt(cmd_params[4]);
					/*
					if(rechargeToOtherCmd7.amount<=999)
						rechargeToOtherCmd7.amount = rechargeToOtherCmd7.amount*1000;
					 */
					requestCmd = rechargeToOtherCmd7;
				} catch (NumberFormatException e) {
					// TODO: handle exception
					cmd_type = CMD_TYPE_WRONG_SYNTAX;
				}
				
			}
			else if(cmd_params[2].equals("2")&&cmd_params[3].equals("1")&&cmd_params[6].equals("")&&!cmd_params[4].equals("")){
				//old
				cmd_type = CMD_TYPE_MOVE_STOCK;
				MoveStockCmd moveStockCmd7 = new MoveStockCmd();
				moveStockCmd7.msisdn = msisdn;
				moveStockCmd7.pinCode = cmd_params[1];
				moveStockCmd7.receiverMsisdn = cmd_params[5];
				try {
					moveStockCmd7.amount = Integer.parseInt(cmd_params[4]);
					/*
					if(moveStockCmd7.amount<=999)
						moveStockCmd7.amount = moveStockCmd7.amount*1000;
					 */
					requestCmd = moveStockCmd7;
				} catch (NumberFormatException e) {
					// TODO: handle exception
					cmd_type = CMD_TYPE_WRONG_SYNTAX;
				}
				
			}
			else{
				cmd_type = CMD_TYPE_WRONG_SYNTAX;
			}
			break;
		default:
			cmd_type = CMD_TYPE_WRONG_SYNTAX;
			break;
		}
	}
}
