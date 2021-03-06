/**
 * 
 */
package com.itpro.etopup.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Vector;
import com.itpro.etopup.struct.AgentInfo;
import com.itpro.etopup.struct.AgentRequest;
import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;
import com.itpro.etopup.struct.DeleteDealerCmd;
import com.itpro.etopup.struct.MTRecord;
import com.itpro.etopup.struct.MoveDealerProvinceCmd;
import com.itpro.etopup.struct.Promotion;
import com.itpro.etopup.struct.Province;
import com.itpro.etopup.struct.RechargeCdrRecord;
import com.itpro.etopup.struct.RefundRechargeCdrRecord;
import com.itpro.etopup.struct.TransactionRecord;
import com.itpro.etopup.struct.dealercmd.BatchRechargeElement;
import com.itpro.etopup.struct.dealercmd.ChangePinCmd;
import com.itpro.etopup.struct.dealercmd.MoveStockCmd;
import com.itpro.etopup.struct.dealercmd.RechargeCmd;
import com.itpro.util.MySQLConnection;
import com.mysql.jdbc.Statement;
import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;

/**
 * @author Giap Van Duc
 *
 */
public class DbConnection extends MySQLConnection {
	
	public DbConnection(String serverIpAddr, String databaseName,
			String userName, String password) {
		super(serverIpAddr, databaseName, userName, password);
		// TODO Auto-generated constructor stub
	}	
	
	public int getPaymentGWTransactionId() throws SQLException{
		PreparedStatement ps=connection.prepareStatement(
		"select next_seq('paymentgw_transaction_id') as transaction_id");
		ps.execute();
		ResultSet rs = ps.getResultSet();
		int result = 0;
		if(rs.next()) {
			result = rs.getInt(1);
		}
		rs.close();
		ps.close();
		return result;
	}
	
	public int getETopupTransactionId() throws SQLException{
		PreparedStatement ps=connection.prepareStatement(
		"select next_seq('etopup_transaction_id') as transaction_id");
		ps.execute();
		ResultSet rs = ps.getResultSet();
		int result = 0;
		if(rs.next()) {
			result = rs.getInt(1);
		}
		rs.close();
		ps.close();
		return result;
	}
	
	public Vector<DealerRequest> getDealerRequestList() throws SQLException {
		// TODO Auto-generated method stub
		Vector<DealerRequest> dealerRequests = new Vector<DealerRequest>();
		PreparedStatement ps=connection.prepareStatement(
				"SELECT id, msisdn, req_date, req_src, content FROM dealer_requests WHERE `status` = 0");
		ps.setMaxRows(30);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		while(rs.next()) {
			DealerRequest dealerRequest = new DealerRequest();
			dealerRequest.id = rs.getInt("id");
			dealerRequest.msisdn = rs.getString("msisdn");
			dealerRequest.req_date = rs.getTimestamp("req_date");
			dealerRequest.req_src = rs.getString("req_src");
			dealerRequest.content = rs.getString("content");
			dealerRequest.status = 0;
			dealerRequests.add(dealerRequest);
		}
		rs.close();
		ps.close();
		if(!dealerRequests.isEmpty()){
			ps=connection.prepareStatement("UPDATE dealer_requests SET status = 1 WHERE id = ?");				
			for(DealerRequest dealerRequest: dealerRequests){				
				ps.setInt(1, dealerRequest.id);					
				ps.addBatch();					
			}
			ps.executeBatch();
			ps.close();
		}			
		return dealerRequests;
	}
	
	public void insertSmsMtRecord(MTRecord mtRecord) throws SQLException{
		/*
		char idx = mtRecord.msisdn.charAt(mtRecord.msisdn.length()-1);
		if(idx<'0'||idx>'9')
			idx='0';
			*/
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("INSERT INTO sms_mt (sms_type,msisdn,content,transaction_id) VALUES (?,?,?,?)");
		ps.setInt(1, mtRecord.sms_type);
		ps.setString(2, mtRecord.msisdn);
		ps.setBytes(3, mtRecord.content);
		ps.setInt(4, mtRecord.transaction_id);
		ps.execute();
		ps.close();
	}

	public DealerInfo getDealerInfo(String msisdn) throws SQLException {
		// TODO Auto-generated method stub
		DealerInfo dealerInfo = null;
		PreparedStatement ps=connection.prepareStatement(
				"select id, msisdn, pin_code, province_register, customer_care_register, account_balance, parent_id, category, active from dealers where msisdn = ? and active IN (1,2,3)");
		ps.setString(1, msisdn);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			dealerInfo = new DealerInfo();
			dealerInfo.id = rs.getInt("id");
			dealerInfo.msisdn = rs.getString("msisdn");
			dealerInfo.pin_code = rs.getString("pin_code");
			dealerInfo.province_register = rs.getInt("province_register");
			dealerInfo.customer_care_register = rs.getInt("customer_care_register");
			dealerInfo.balance = rs.getInt("account_balance");
			dealerInfo.parent_id = rs.getInt("parent_id");
			dealerInfo.category = rs.getInt("category");
			dealerInfo.active = rs.getInt("active");
		}
		rs.close();
		ps.close();
		return dealerInfo;
	}
	
	public DealerInfo getDealerInfo(int dealerId) throws SQLException {
		// TODO Auto-generated method stub
		DealerInfo dealerInfo = null;
		PreparedStatement ps=connection.prepareStatement(
				"select id, msisdn, pin_code, province_register, customer_care_register, account_balance, parent_id, category, active from dealers where id = ? ");
		ps.setInt(1, dealerId);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			dealerInfo = new DealerInfo();
			dealerInfo.id = rs.getInt("id");
			dealerInfo.msisdn = rs.getString("msisdn");
			dealerInfo.pin_code = rs.getString("pin_code");
			dealerInfo.province_register = rs.getInt("province_register");
			dealerInfo.customer_care_register = rs.getInt("customer_care_register");
			dealerInfo.balance = rs.getInt("account_balance");
			dealerInfo.parent_id = rs.getInt("parent_id");
			dealerInfo.category = rs.getInt("category");
			dealerInfo.active = rs.getInt("active");
		}
		rs.close();
		ps.close();
		return dealerInfo;
	}
	
	public AgentInfo getAgentInfo(int agentId) throws SQLException {
		// TODO Auto-generated method stub
		AgentInfo agentInfo = null;
		PreparedStatement ps=connection.prepareStatement(
				"select username, province_code, customer_care, sys from users where id = ? ");
		ps.setInt(1, agentId);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			agentInfo = new AgentInfo();
			agentInfo.id = agentId;
			agentInfo.user_name = rs.getString("username");
			agentInfo.province_code = rs.getInt("province_code");
			agentInfo.customer_care = rs.getInt("customer_care");
			agentInfo.sys_type = rs.getString("sys");
		}
		rs.close();
		ps.close();
		return agentInfo;
	}
	
	public TransactionRecord getTransactionRecord(int id) throws SQLException {
		// TODO Auto-generated method stub
        TransactionRecord transactionRecord = null;
        PreparedStatement ps=connection.prepareStatement(
                "select id, date_time, type, dealer_msisdn, dealer_id, balance_changed_amount, balance_before, balance_after, "
                    + "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, refund_status, result_description,recharge_msidn,recharge_value,batch_recharge_id, refer_transaction_id from transactions where id=?");
        ps.setInt(1, id);
        ps.execute();
        ResultSet rs = ps.getResultSet();
        if(rs.next()) {
            transactionRecord = new TransactionRecord();
            transactionRecord.id = rs.getInt("id");
            transactionRecord.date_time = rs.getTimestamp("date_time");
            transactionRecord.type = rs.getInt("type");
            transactionRecord.dealer_msisdn = rs.getString("dealer_msisdn");
            transactionRecord.dealer_id = rs.getInt("dealer_id");
            transactionRecord.balance_changed_amount = rs.getLong("balance_changed_amount");
            transactionRecord.balance_before = rs.getLong("balance_before");
            transactionRecord.balance_after = rs.getLong("balance_after");
            transactionRecord.partner_msisdn = rs.getString("partner_msisdn");
            transactionRecord.partner_id = rs.getInt("partner_id");
            transactionRecord.partner_balance_before = rs.getLong("partner_balance_before");
            transactionRecord.partner_balance_after = rs.getLong("partner_balance_after");
            transactionRecord.status = rs.getInt("status");
            transactionRecord.refund_status = rs.getInt("refund_status");
            transactionRecord.recharge_msidn= rs.getString("recharge_msidn");
            transactionRecord.recharge_value= rs.getInt("recharge_value");
            transactionRecord.result_description = rs.getString("result_description");
            transactionRecord.batch_recharge_id=rs.getInt("batch_recharge_id");
            transactionRecord.refer_transaction_id=rs.getInt("refer_transaction_id");
        }
        rs.close();
        ps.close();
        return transactionRecord;
    }
    
	public void updateDealerRequest(DealerRequest dealerRequest) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "UPDATE dealer_requests SET status = 2, cmd_type=?, result=?";;
		if(dealerRequest.dealer_id!=0)
			sql+=", dealer_id=?";
		if(dealerRequest.transaction_id!=0)
			sql+=", transaction_id=?";
		for(int i=0;i<dealerRequest.cmd_params.length;i++){
			sql+=", cmd_param"+i+"=?";
		}
		sql += " WHERE id=?";
		PreparedStatement ps = null;
		ps=connection.prepareStatement(sql);
		ps.setInt(1, dealerRequest.cmd_type);
		ps.setString(2, dealerRequest.result);
		int index = 2;
		if(dealerRequest.dealer_id!=0)
			ps.setInt(++index, dealerRequest.dealer_id);
	
		if(dealerRequest.transaction_id!=0)
			ps.setInt(++index, dealerRequest.transaction_id);
		for(int i=0;i<dealerRequest.cmd_params.length;i++){
			ps.setString(++index, dealerRequest.cmd_params[i]);
		}
		ps.setInt(++index, dealerRequest.id);
		ps.execute();
		ps.close();
	}

	public void changePIN(ChangePinCmd changePinCmd) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "UPDATE dealers SET pin_code = ? WHERE id=?";
		PreparedStatement ps = connection.prepareStatement(sql);
		ps.setString(1, changePinCmd.newPin);
		ps.setInt(2, changePinCmd.dealerInfo.id);
		ps.execute();
		ps.close();
	}

	public void moveStock(MoveStockCmd moveStockCmd) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "{call move_stock (?, ?, ?, ?, ?, ?)}";
		CallableStatement stmt = null;
		stmt = connection.prepareCall(sql);
		stmt.setInt(1, moveStockCmd.dealerInfo.id);
		stmt.setInt(2, moveStockCmd.receiverInfo.id);
		stmt.setInt(3, moveStockCmd.amount);
		stmt.registerOutParameter(4, java.sql.Types.INTEGER);
		stmt.registerOutParameter(5, java.sql.Types.INTEGER);
		stmt.registerOutParameter(6, java.sql.Types.INTEGER);
		stmt.execute();
		moveStockCmd.db_return_code = stmt.getInt(4);
		moveStockCmd.balanceAfter = stmt.getInt(5);
		moveStockCmd.receiverBalanceAfter = stmt.getInt(6);
		stmt.close();
	}

	public void insertTransactionRecord(TransactionRecord transactionRecord) throws SQLException {
		// TODO Auto-generated method stub
		String sql;
		PreparedStatement ps;
		switch (transactionRecord.type){
		
		//Transaction for Dealer Request
		case TransactionRecord.TRANS_TYPE_STOCK_MOVE_OUT:
		case TransactionRecord.TRANS_TYPE_STOCK_MOVE_IN:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, refer_transaction_id, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.dealer_category);
			ps.setLong(8, transactionRecord.transaction_amount_req);
			ps.setLong(9, transactionRecord.balance_changed_amount);
			ps.setLong(10, transactionRecord.balance_before);
			ps.setLong(11, transactionRecord.balance_after);
			ps.setString(12, transactionRecord.partner_msisdn);
			ps.setInt(13, transactionRecord.partner_id);
			ps.setLong(14, transactionRecord.partner_balance_before);
			ps.setLong(15, transactionRecord.partner_balance_after);
			ps.setInt(16, transactionRecord.refer_transaction_id);
			ps.setInt(17, transactionRecord.status);
			ps.setString(18, transactionRecord.result_description);
			ps.setInt(19, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_RECHARGE_VOUCHER:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.dealer_category);
			ps.setLong(8, transactionRecord.transaction_amount_req);
			ps.setLong(9, transactionRecord.balance_changed_amount);
			ps.setLong(10, transactionRecord.balance_before);
			ps.setLong(11, transactionRecord.balance_after);
			ps.setString(12, transactionRecord.recharge_msidn);
			ps.setInt(13, transactionRecord.recharge_value);
			ps.setInt(14, transactionRecord.recharge_sub_type);
			ps.setInt(15, transactionRecord.status);
			ps.setString(16, transactionRecord.result_description);
			ps.setInt(17, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_BULK_RECHARGE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description,`batch_recharge_id`,`batch_recharge_succes`,`batch_recharge_fail`, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.dealer_category);
			ps.setLong(8, transactionRecord.transaction_amount_req);
			ps.setLong(9, transactionRecord.balance_changed_amount);
			ps.setLong(10, transactionRecord.balance_before);
			ps.setLong(11, transactionRecord.balance_after);
			ps.setString(12, transactionRecord.recharge_msidn);
			ps.setInt(13, transactionRecord.recharge_value);
			ps.setInt(14, transactionRecord.recharge_sub_type);
			ps.setInt(15, transactionRecord.status);
			ps.setString(16, transactionRecord.result_description);
			ps.setInt(17, transactionRecord.batch_recharge_id);
			ps.setInt(18, transactionRecord.batch_recharge_succes);
			ps.setInt(19, transactionRecord.batch_recharge_fail);
			ps.setInt(20, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
			
		//Transactions for Agent Request	
		case TransactionRecord.TRANS_TYPE_ADD_DEALER:
		case TransactionRecord.TRANS_TYPE_CANCEL_DEALER:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, customer_care, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, remark, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setInt(8, transactionRecord.dealer_category);
			ps.setLong(9, transactionRecord.transaction_amount_req);
			ps.setLong(10, transactionRecord.balance_changed_amount);
			ps.setLong(11, transactionRecord.balance_before);
			ps.setLong(12, transactionRecord.balance_after);
			ps.setString(13, transactionRecord.agent);
			ps.setInt(14, transactionRecord.agent_id);
			ps.setString(15, transactionRecord.approved);
			ps.setInt(16, transactionRecord.approved_id);
			ps.setString(17, transactionRecord.remark);
			ps.setInt(18, transactionRecord.status);
			ps.setString(19, transactionRecord.result_description);
			ps.setInt(20, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_ADD_RETAILER:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_parent_id, dealer_province, customer_care, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, remark, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_parent_id);
			ps.setInt(7, transactionRecord.dealer_province);
			ps.setInt(8, transactionRecord.customer_care);
			ps.setInt(9, transactionRecord.dealer_category);
			ps.setLong(10, transactionRecord.transaction_amount_req);
			ps.setLong(11, transactionRecord.balance_changed_amount);
			ps.setLong(12, transactionRecord.balance_before);
			ps.setLong(13, transactionRecord.balance_after);
			ps.setString(14, transactionRecord.agent);
			ps.setInt(15, transactionRecord.agent_id);
			ps.setString(16, transactionRecord.approved);
			ps.setInt(17, transactionRecord.approved_id);
			ps.setString(18, transactionRecord.remark);
			ps.setInt(19, transactionRecord.status);
			ps.setString(20, transactionRecord.result_description);
			ps.setInt(21, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_STOCK_ALLOCATION:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, customer_care, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, cash_value, commision_rate, commision_value, promotion_value, iv_cash_kip, iv_cash_baht, iv_cash_usd, iv_cash_yuan, iv_rate_baht, iv_rate_usd, iv_rate_yuan, iv_paymode, iv_payinfo, refer_transaction_id, remark, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setInt(8, transactionRecord.dealer_category);
			ps.setLong(9, transactionRecord.transaction_amount_req);
			ps.setLong(10, transactionRecord.balance_changed_amount);
			ps.setLong(11, transactionRecord.balance_before);
			ps.setLong(12, transactionRecord.balance_after);
			ps.setString(13, transactionRecord.agent);
			ps.setInt(14, transactionRecord.agent_id);
			ps.setString(15, transactionRecord.approved);
			ps.setInt(16, transactionRecord.approved_id);
			ps.setLong(17, transactionRecord.addBalanceInfo.cash_value);
			ps.setDouble(18, transactionRecord.addBalanceInfo.commision_rate);
			ps.setLong(19, transactionRecord.addBalanceInfo.commision_value);
			ps.setLong(20, transactionRecord.addBalanceInfo.promotion_value);
			ps.setDouble(21, transactionRecord.addBalanceInfo.iv_cash_kip);
			ps.setDouble(22, transactionRecord.addBalanceInfo.iv_cash_baht);
			ps.setDouble(23, transactionRecord.addBalanceInfo.iv_cash_usd);
			ps.setDouble(24, transactionRecord.addBalanceInfo.iv_cash_yuan);
			ps.setDouble(25, transactionRecord.addBalanceInfo.iv_rate_baht);
			ps.setDouble(26, transactionRecord.addBalanceInfo.iv_rate_usd);
			ps.setDouble(27, transactionRecord.addBalanceInfo.iv_rate_yuan);
			ps.setInt(28, transactionRecord.addBalanceInfo.iv_paymode);
			ps.setString(29, transactionRecord.addBalanceInfo.iv_payinfo);
			ps.setInt(30, transactionRecord.refer_transaction_id);
			ps.setString(31, transactionRecord.remark);
			ps.setInt(32, transactionRecord.status);
			ps.setString(33, transactionRecord.result_description);
			ps.setInt(34, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_CANCEL_RECHARGE_VOUCHER:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, customer_care, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description,agent,agent_id,approved, approved_id,refer_transaction_id,batch_recharge_id, remark, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setInt(8, transactionRecord.dealer_category);
			ps.setLong(9, transactionRecord.transaction_amount_req);
			ps.setLong(10, transactionRecord.balance_changed_amount);
			ps.setLong(11, transactionRecord.balance_before);
			ps.setLong(12, transactionRecord.balance_after);
			ps.setString(13, transactionRecord.recharge_msidn);
			ps.setInt(14, transactionRecord.recharge_value);
			ps.setInt(15, transactionRecord.recharge_sub_type);
			ps.setInt(16, transactionRecord.status);
			ps.setString(17, transactionRecord.result_description);
			ps.setString(18, transactionRecord.agent);
			ps.setInt(19, transactionRecord.agent_id);
			ps.setString(20, transactionRecord.approved);
			ps.setInt(21, transactionRecord.approved_id);
			ps.setInt(22, transactionRecord.refer_transaction_id);
			ps.setInt(23, transactionRecord.batch_recharge_id);
			ps.setString(24, transactionRecord.remark);
			ps.setInt(25, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;	
		case TransactionRecord.TRANS_TYPE_CANCEL_STOCK_MOVE_OUT:
		case TransactionRecord.TRANS_TYPE_CANCEL_STOCK_MOVE_IN:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, customer_care, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, result_description,agent,agent_id,approved, approved_id,refer_transaction_id,remark,service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setInt(8, transactionRecord.dealer_category);
			ps.setLong(9, transactionRecord.transaction_amount_req);
			ps.setLong(10, transactionRecord.balance_changed_amount);
			ps.setLong(11, transactionRecord.balance_before);
			ps.setLong(12, transactionRecord.balance_after);
			ps.setString(13, transactionRecord.partner_msisdn);
			ps.setInt(14, transactionRecord.partner_id);
			ps.setLong(15, transactionRecord.partner_balance_before);
			ps.setLong(16, transactionRecord.partner_balance_after);
			ps.setInt(17, transactionRecord.status);
			ps.setString(18, transactionRecord.result_description);
			ps.setString(19, transactionRecord.agent);
			ps.setInt(20, transactionRecord.agent_id);
			ps.setString(21, transactionRecord.approved);
			ps.setInt(22, transactionRecord.approved_id);
			ps.setInt(23, transactionRecord.refer_transaction_id);
			ps.setString(24, transactionRecord.remark);
			ps.setInt(25, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;  
		case TransactionRecord.TRANS_TYPE_CANCEL_STOCK_ALLOCATION:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, customer_care, dealer_category, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, refer_transaction_id, remark, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setInt(8, transactionRecord.dealer_category);
			ps.setLong(9, transactionRecord.transaction_amount_req);
			ps.setLong(10, transactionRecord.balance_changed_amount);
			ps.setLong(11, transactionRecord.balance_before);
			ps.setLong(12, transactionRecord.balance_after);
			ps.setString(13, transactionRecord.agent);
			ps.setInt(14, transactionRecord.agent_id);
			ps.setString(15, transactionRecord.approved);
			ps.setInt(16, transactionRecord.approved_id);
			ps.setInt(17, transactionRecord.refer_transaction_id);
			ps.setString(18, transactionRecord.remark);
			ps.setInt(19, transactionRecord.status);
			ps.setString(20, transactionRecord.result_description);
			ps.setInt(21, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_MOVE_OUT_DEALER_PROVINCE_SOURCE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_category, customer_care, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, dealer_province, dealer_new_id, dealer_new_province, refer_transaction_id, remark, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_category);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setLong(8, transactionRecord.transaction_amount_req);
			ps.setLong(9, transactionRecord.balance_changed_amount);
			ps.setLong(10, transactionRecord.balance_before);
			ps.setLong(11, transactionRecord.balance_after);
			ps.setString(12, transactionRecord.agent);
			ps.setInt(13, transactionRecord.agent_id);
			ps.setString(14, transactionRecord.approved);
			ps.setInt(15, transactionRecord.approved_id);
			ps.setInt(16, transactionRecord.dealer_province);
			ps.setInt(17, transactionRecord.dealer_new_id);
			ps.setInt(18, transactionRecord.dealer_new_province);
			ps.setInt(19, transactionRecord.refer_transaction_id);
			ps.setString(20, transactionRecord.remark);
			ps.setInt(21, transactionRecord.status);
			ps.setString(22, transactionRecord.result_description);
			ps.setInt(23, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break; 	
		case TransactionRecord.TRANS_TYPE_MOVE_IN_DEALER_PROVINCE_DESTINATION:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_category, customer_care, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, dealer_province, refer_transaction_id, remark, status, result_description, service_trans_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_category);
			ps.setInt(7, transactionRecord.customer_care);
			ps.setLong(8, transactionRecord.transaction_amount_req);
			ps.setLong(9, transactionRecord.balance_changed_amount);
			ps.setLong(10, transactionRecord.balance_before);
			ps.setLong(11, transactionRecord.balance_after);
			ps.setString(12, transactionRecord.agent);
			ps.setInt(13, transactionRecord.agent_id);
			ps.setString(14, transactionRecord.approved);
			ps.setInt(15, transactionRecord.approved_id);
			ps.setInt(16, transactionRecord.dealer_province);
			ps.setInt(17, transactionRecord.refer_transaction_id);
			ps.setString(18, transactionRecord.remark);
			ps.setInt(19, transactionRecord.status);
			ps.setString(20, transactionRecord.result_description);
			ps.setInt(21, transactionRecord.service_trans_id);
			ps.execute();
			ps.close();
			break; 
			
		default:
			break;
		}
	}
	
    public void updateTransactionRecord(TransactionRecord transactionRecord) throws SQLException {
        // TODO Auto-generated method stub
        PreparedStatement ps = null;        
        ps=connection.prepareStatement("UPDATE `transactions` SET `refund_status`=? WHERE id=?");
        ps.setInt(1, transactionRecord.refund_status);
        ps.setInt(2, transactionRecord.id);
        ps.execute();
        ps.close();
    }
	public void deductBalance(RechargeCmd rechargeCmd) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "{call deduct_balance (?, ?, ?, ?)}";
		CallableStatement stmt = null;
		stmt = connection.prepareCall(sql);
		stmt.setInt(1, rechargeCmd.dealerInfo.id);
		stmt.setInt(2, rechargeCmd.amount);
		stmt.registerOutParameter(3, java.sql.Types.INTEGER);
		stmt.registerOutParameter(4, java.sql.Types.INTEGER);
		stmt.execute();
		rechargeCmd.db_return_code = stmt.getInt(3);
		rechargeCmd.balanceAfter = stmt.getInt(4);
		stmt.close();
	}
    public void deductBalance(BatchRechargeElement batchRechargeElement) throws SQLException {
        // TODO Auto-generated method stub
        String sql = "{call deduct_balance (?, ?, ?, ?)}";
        CallableStatement stmt = null;
        stmt = connection.prepareCall(sql);
        stmt.setInt(1, batchRechargeElement.dealer_id);
        stmt.setInt(2, batchRechargeElement.recharge_value);
        stmt.registerOutParameter(3, java.sql.Types.INTEGER);
        stmt.registerOutParameter(4, java.sql.Types.INTEGER);
        stmt.execute();
        batchRechargeElement.db_return_code = stmt.getInt(3);
        batchRechargeElement.balanceAfter = stmt.getInt(4);
        stmt.close();
    }
	public void insertUssdNotifyRecord(MTRecord mtRecord) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("INSERT INTO ussd_notify (msisdn,content,transaction_id) VALUES (?,?,?)");
		ps.setString(1, mtRecord.msisdn);
		ps.setBytes(2, mtRecord.content);
		ps.setInt(3, mtRecord.transaction_id);
		ps.execute();
		ps.close();
	}

	public Vector<AgentRequest> getAgentRequestList() throws SQLException {
		// TODO Auto-generated method stub
		Vector<AgentRequest> agentRequests = new Vector<AgentRequest>();
		PreparedStatement ps=connection.prepareStatement(
				"SELECT id, req_type, req_date, agent_username, agent_id, approve_id, dealer_msisdn, dealer_contact_phone, dealer_name, dealer_parent_id, dealer_province_code, customer_care, dealer_id_card_number, "
				+ "dealer_birthdate, dealer_address, cash_value, commision_rate, commision_value, promotion_value, iv_cash_kip, iv_cash_baht, iv_cash_usd, iv_cash_yuan, iv_rate_baht, iv_rate_usd, iv_rate_yuan, "
				+ "iv_paymode, iv_payinfo, refund_transaction_id,refund_msisdn,refund_amount,category, remark FROM agent_requests WHERE status = 0");
		ps.setMaxRows(30);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		while(rs.next()) {
			AgentRequest agentRequest = new AgentRequest();
			agentRequest.id = rs.getInt("id");
			agentRequest.req_type = rs.getInt("req_type");
			agentRequest.req_date = rs.getTimestamp("req_date");
			agentRequest.agent_id = rs.getInt("agent_id");
			agentRequest.agent_approved_id = rs.getInt("approve_id");
			agentRequest.dealer_msisdn = rs.getString("dealer_msisdn");
			agentRequest.dealer_contact_phone = rs.getString("dealer_contact_phone");
			try {
				byte[] dealer_name = rs.getBytes("dealer_name");
				if(dealer_name!=null)
					agentRequest.dealer_name = new String(dealer_name, "UTF-8");
				else
					agentRequest.dealer_name = null;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				agentRequest.dealer_name = rs.getString("dealer_name");
			}
			agentRequest.dealer_parent_id=rs.getInt("dealer_parent_id");
			agentRequest.dealer_province_code = rs.getInt("dealer_province_code");
			agentRequest.customer_care = rs.getInt("customer_care");
			agentRequest.dealer_id_card_number = rs.getString("dealer_id_card_number");
			try {
				agentRequest.dealer_birthdate = rs.getDate("dealer_birthdate");
			} catch (Exception e) {
				// TODO: handle exception
				agentRequest.dealer_birthdate = null;
			}

			try {
				byte[] dealer_address = rs.getBytes("dealer_address");
				if(dealer_address!=null)
					agentRequest.dealer_address = new String(dealer_address,"UTF-8");
				else
					agentRequest.dealer_address = null;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				agentRequest.dealer_address = rs.getString("dealer_address");
			}
			
			//cash_value, commision_value, iv_cash_kip, iv_cash_baht, iv_cash_usd, iv_cash_yuan, iv_rate_baht, iv_rate_usd, iv_rate_yuan
			
			agentRequest.addBalanceInfo.cash_value = rs.getLong("cash_value");
			agentRequest.addBalanceInfo.commision_rate = rs.getDouble("commision_rate");
			agentRequest.addBalanceInfo.commision_value = rs.getLong("commision_value");
			agentRequest.addBalanceInfo.promotion_value = rs.getLong("promotion_value");
			agentRequest.addBalanceInfo.iv_cash_kip = rs.getDouble("iv_cash_kip");
			agentRequest.addBalanceInfo.iv_cash_baht = rs.getDouble("iv_cash_baht");
			agentRequest.addBalanceInfo.iv_cash_usd = rs.getDouble("iv_cash_usd");
			agentRequest.addBalanceInfo.iv_cash_yuan = rs.getDouble("iv_cash_yuan");
			agentRequest.addBalanceInfo.iv_rate_baht = rs.getDouble("iv_rate_baht");
			agentRequest.addBalanceInfo.iv_rate_usd = rs.getDouble("iv_rate_usd");
			agentRequest.addBalanceInfo.iv_rate_yuan = rs.getDouble("iv_rate_yuan");
			agentRequest.addBalanceInfo.iv_paymode = rs.getInt("iv_paymode");
			agentRequest.addBalanceInfo.iv_payinfo = rs.getString("iv_payinfo");
			
			agentRequest.refund_transaction_id = rs.getInt("refund_transaction_id");
			agentRequest.refund_msisdn=rs.getString("refund_msisdn");
			agentRequest.refund_amount=rs.getInt("refund_amount");
			agentRequest.category=rs.getInt("category");
			agentRequest.remark=rs.getString("remark");
			agentRequest.status = 0;
			agentRequests.add(agentRequest);
		}
		rs.close();
		ps.close();
		if(!agentRequests.isEmpty()){
			ps=connection.prepareStatement("UPDATE agent_requests SET `status` = 1 WHERE id = ?");				
			for(AgentRequest agentRequest: agentRequests){				
				ps.setInt(1, agentRequest.id);					
				ps.addBatch();					
			}
			ps.executeBatch();
			ps.close();
		}			
		return agentRequests;
	}

	public void insertDealer(DealerInfo dealerInfo) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("INSERT INTO dealers ("
				+ "msisdn, pin_code, register_date, agent_init, agent_init_id, agent_approved, agent_approved_id, name, contact_phone, parent_id, birth_date, id_card_number, province_register, customer_care_register, address, "
				+ "account_balance, active,category) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
		ps.setString(1, dealerInfo.msisdn);
		ps.setString(2, dealerInfo.pin_code);
		ps.setTimestamp(3, dealerInfo.register_date);
		ps.setString(4, dealerInfo.agent_init);
		ps.setInt(5, dealerInfo.agent_init_id);
		ps.setString(6, dealerInfo.agent_approved);
		ps.setInt(7, dealerInfo.agent_approved_id);
		try {
			if(dealerInfo.name!=null)
				ps.setBytes(8, dealerInfo.name.getBytes("UTF-8"));
			else
				ps.setString(8,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(8, dealerInfo.name);
		}
		
		try {
			if(dealerInfo.contact_phone!=null)
				ps.setBytes(9, dealerInfo.contact_phone.getBytes("UTF-8"));
			else
				ps.setString(9,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(9, dealerInfo.contact_phone);
		}
		
		ps.setInt(10, dealerInfo.parent_id);
		ps.setDate(11, dealerInfo.birth_date);
		ps.setString(12, dealerInfo.id_card_number);
		ps.setInt(13, dealerInfo.province_register);
		ps.setInt(14, dealerInfo.customer_care_register);
		try {
			if(dealerInfo.address!=null)
				ps.setBytes(15, dealerInfo.address.getBytes("UTF-8"));
			else
				ps.setString(15,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(15, dealerInfo.address);
		}
		ps.setLong(16, dealerInfo.balance);
		ps.setInt(17, dealerInfo.active);
	    ps.setInt(18, dealerInfo.category);
		ps.executeUpdate();

		ResultSet rs = ps.getGeneratedKeys();
		if (rs.next()){
			dealerInfo.id=rs.getInt(1);
		}
		else{
			dealerInfo.id=0;
		}
		rs.close();
		ps.close();
	}

	public void deleteDealer(DeleteDealerCmd deleteDealerCmd) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "{call delete_dealer (?, ?)}";
		CallableStatement stmt = null;
		stmt = connection.prepareCall(sql);
		stmt.setInt(1, deleteDealerCmd.dealer_id);
		stmt.registerOutParameter(2, java.sql.Types.INTEGER);
		stmt.execute();
		deleteDealerCmd.db_return_code = stmt.getInt(2);
		stmt.close();
	}
	
	public void updateAgentRequest(AgentRequest agentRequest) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "UPDATE agent_requests SET status=?, agent_username=?, approve_username =?, result_code = ?, result_description=?";;
		if(agentRequest.dealer_id!=0)
			sql+=", dealer_id=?";
		if(agentRequest.transaction_id!=0)
			sql+=", transaction_id=?";
		sql+=", balance_add_amount=?";
		sql += " WHERE id=?";
		PreparedStatement ps = null;
		ps=connection.prepareStatement(sql);
		ps.setInt(1, agentRequest.status);
		ps.setString(2, agentRequest.agentInit!=null?agentRequest.agentInit.user_name:null);
		ps.setString(3, agentRequest.agentApproved!=null?agentRequest.agentApproved.user_name:null);
		ps.setInt(4, agentRequest.result_code);
		ps.setString(5, AgentRequest.resultString[agentRequest.result_code]);
		int index = 5;
		if(agentRequest.dealer_id!=0)
			ps.setInt(++index, agentRequest.dealer_id);
	
		if(agentRequest.transaction_id!=0)
			ps.setInt(++index, agentRequest.transaction_id);
		ps.setLong(++index, agentRequest.balance_add_amount);
		ps.setInt(++index, agentRequest.id);
		ps.execute();
		ps.close();
	}

	public void updateDealer(AgentRequest agentRequest) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "UPDATE dealers SET account_balance = account_balance + ? WHERE id = ?";
		PreparedStatement ps = null;
		ps=connection.prepareStatement(sql);
		ps.setLong(1, agentRequest.balance_add_amount);
		ps.setInt(2, agentRequest.dealer_id);
		ps.execute();
		ps.close();
	}

	public Vector<BatchRechargeElement> getBatchRechargeElementList(int batchRechargeId) throws SQLException {
		// TODO Auto-generated method stub
		Vector<BatchRechargeElement> batchRechargeElements = new Vector<BatchRechargeElement>();
		PreparedStatement ps=connection.prepareStatement(
				"SELECT id, dealer_id, dealer_msisdn, recharge_msisdn, recharge_value FROM batch_recharge WHERE batch_recharge_id = ? AND `status` = 0");
		ps.setInt(1, batchRechargeId);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		while(rs.next()) {
			BatchRechargeElement batchRechargeElement = new BatchRechargeElement();
			batchRechargeElement.id = rs.getInt("id");
			batchRechargeElement.dealer_id = rs.getInt("dealer_id");
			batchRechargeElement.dealer_msisdn = rs.getString("dealer_msisdn");
			batchRechargeElement.recharge_msisdn = rs.getString("recharge_msisdn");
			batchRechargeElement.recharge_value = rs.getInt("recharge_value");
			batchRechargeElement.batch_recharge_id = batchRechargeId;
			batchRechargeElement.status = 0;
			batchRechargeElements.add(batchRechargeElement);
		}
		rs.close();
		ps.close();
		if(!batchRechargeElements.isEmpty()){
			ps=connection.prepareStatement("UPDATE batch_recharge SET status = 1 WHERE id = ?");				
			for(BatchRechargeElement batchRechargeElement: batchRechargeElements){				
				ps.setInt(1, batchRechargeElement.id);					
				ps.addBatch();					
			}
			ps.executeBatch();
			ps.close();
		}			
		return batchRechargeElements;
	}
    public Vector<BatchRechargeElement> getRefundBatchRechargeElementList(int batchRechargeId) throws SQLException {
        // TODO Auto-generated method stub
        Vector<BatchRechargeElement> batchRechargeElements = new Vector<BatchRechargeElement>();
        PreparedStatement ps=connection.prepareStatement(
                "SELECT id, dealer_id, dealer_msisdn, recharge_msisdn, recharge_value,status,result_code,result_string FROM batch_recharge WHERE batch_recharge_id = ? AND `refund_status` = 0");
        ps.setInt(1, batchRechargeId);
        ps.execute();
        ResultSet rs = ps.getResultSet();
        while(rs.next()) {
            BatchRechargeElement batchRechargeElement = new BatchRechargeElement();
            batchRechargeElement.id = rs.getInt("id");
            batchRechargeElement.dealer_id = rs.getInt("dealer_id");
            batchRechargeElement.dealer_msisdn = rs.getString("dealer_msisdn");
            batchRechargeElement.recharge_msisdn = rs.getString("recharge_msisdn");
            batchRechargeElement.recharge_value = rs.getInt("recharge_value");
            batchRechargeElement.batch_recharge_id = batchRechargeId;
            batchRechargeElement.status = rs.getInt("status");
            batchRechargeElement.result_code= rs.getInt("result_code");
            batchRechargeElement.result_string= rs.getString("result_string");
            batchRechargeElements.add(batchRechargeElement);
        }
        rs.close();
        ps.close();
        if(!batchRechargeElements.isEmpty()){
            ps=connection.prepareStatement("UPDATE batch_recharge SET refund_status = 1 WHERE id = ?");                
            for(BatchRechargeElement batchRechargeElement: batchRechargeElements){              
                ps.setInt(1, batchRechargeElement.id);                  
                ps.addBatch();                  
            }
            ps.executeBatch();
            ps.close();
        }           
        return batchRechargeElements;
    }
    public  BatchRechargeElement getRefundBatchRechargeElement(int batchRechargeId,String recharge_msisdn) throws SQLException {
        // TODO Auto-generated method stub
        BatchRechargeElement batchRechargeElement =null;
        PreparedStatement ps=connection.prepareStatement(
                "SELECT id, dealer_id, dealer_msisdn, recharge_msisdn, recharge_value,status,result_code,result_string FROM batch_recharge WHERE batch_recharge_id = ? AND recharge_msisdn=? AND `refund_status` = 0");
        ps.setInt(1, batchRechargeId);
        ps.setString(2, recharge_msisdn);
        ps.execute();
        ResultSet rs = ps.getResultSet();
        if(rs.next()) {
            batchRechargeElement = new BatchRechargeElement();
            batchRechargeElement.id = rs.getInt("id");
            batchRechargeElement.dealer_id = rs.getInt("dealer_id");
            batchRechargeElement.dealer_msisdn = rs.getString("dealer_msisdn");
            batchRechargeElement.recharge_msisdn = rs.getString("recharge_msisdn");
            batchRechargeElement.recharge_value = rs.getInt("recharge_value");
            batchRechargeElement.batch_recharge_id = batchRechargeId;
            batchRechargeElement.status = rs.getInt("status");
            batchRechargeElement.result_code= rs.getInt("result_code");
            batchRechargeElement.result_string= rs.getString("result_string");
        }
        rs.close();
        ps.close();
//        if(batchRechargeElement!=null){
//            ps=connection.prepareStatement("UPDATE batch_recharge SET refund_status = 1 WHERE id = ?");                
//            ps.setInt(1, batchRechargeElement.id);
//            ps.execute();
//            ps.close();
//        }           
        return batchRechargeElement;
    }
    public void updateBatchRechargeElement(BatchRechargeElement batchRechargeElement) throws SQLException {
        // TODO Auto-generated method stub
        PreparedStatement ps = null;        
        ps=connection.prepareStatement("UPDATE batch_recharge SET `status`=?, `result_code`=?,`result_string`=?,`refund_status`=?,`refund_result_code`=?,`refund_result_string`=? WHERE id=?");
        ps.setInt(1, batchRechargeElement.status);
        ps.setInt(2, batchRechargeElement.result_code);
        ps.setString(3, batchRechargeElement.result_string);
        ps.setInt(4, batchRechargeElement.refund_status);
        ps.setInt(5, batchRechargeElement.refund_result_code);
        ps.setString(6, batchRechargeElement.refund_result_string);
        ps.setInt(7, batchRechargeElement.id);
        ps.execute();
        ps.close();
    }


    public void insertRefundCdrRecord(RefundRechargeCdrRecord refundRechargeCdrRecord) throws SQLException {
        // TODO Auto-generated method stub
        PreparedStatement ps = null;
        String sql = "INSERT `refund_cdr`(`date_time`,`dealer_msisdn`,`dealer_id`,`dealer_province`,`customer_care`,`dealer_category`,`msisdn`,`receiver_province`,`charge_value`,`result_code`,`result_string`,`status`,`payment_transaction_id`,`spID`,`serviceID`,`transaction_id`, `type`, `receiver_sub_type`, `balance_changed_amount`, `balance_before`, `balance_after`)"
                + " VALUES (now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        ps = connection.prepareStatement(sql);
        ps.setString(1, refundRechargeCdrRecord.dealer_msisdn);
        ps.setInt(2, refundRechargeCdrRecord.dealer_id);
        ps.setInt(3, refundRechargeCdrRecord.dealer_province);
        ps.setInt(4, refundRechargeCdrRecord.customer_care);
        ps.setInt(5, refundRechargeCdrRecord.dealer_category);
        ps.setString(6, refundRechargeCdrRecord.msisdn);
        ps.setInt(7, refundRechargeCdrRecord.receiver_province);
        ps.setLong(8, refundRechargeCdrRecord.charge_value);
        ps.setInt(9, refundRechargeCdrRecord.result_code);
        ps.setString(10, refundRechargeCdrRecord.result_string);
        ps.setInt(11, refundRechargeCdrRecord.status);
        ps.setInt(12, refundRechargeCdrRecord.payment_transaction_id);
        ps.setString(13, refundRechargeCdrRecord.spID);
        ps.setString(14, refundRechargeCdrRecord.serviceID);
        ps.setInt(15, refundRechargeCdrRecord.transaction_id);
        ps.setInt(16, refundRechargeCdrRecord.type);
        ps.setInt(17, refundRechargeCdrRecord.receiver_sub_type);
        ps.setLong(18, refundRechargeCdrRecord.balance_changed_amount);
        ps.setLong(19, refundRechargeCdrRecord.balance_before);
        ps.setLong(20, refundRechargeCdrRecord.balance_after);
        ps.execute();
        ps.close();
    }
    public void insertRechargeCdr(RechargeCdrRecord rechargeCdrRecord) throws SQLException {
        // TODO Auto-generated method stub
        PreparedStatement ps = null;
        String sql = "INSERT INTO `recharge_cdr`(`payment_transaction_id`,`date_time`,`type`,`dealer_msisdn`,`dealer_id`,`dealer_province`,`dealer_category`,`balance_changed_amount`,`balance_before`,`balance_after`,`receiver_msidn`,`receiver_province`,`receiver_sub_type`,`receiver_active_date`,`receiver_new_expire_date`,`recharge_value`,`bonus_balance`,`bonus_data`,`promotion_balance_id`,`promotion_data_id`,`transaction_id`,`result`,`result_code`,`result_description`) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        ps=connection.prepareStatement(sql);
        ps.setInt(1,rechargeCdrRecord.payment_transaction_id);
        ps.setTimestamp(2,rechargeCdrRecord.date_time);
        ps.setInt(3,rechargeCdrRecord.type);
        ps.setString(4,rechargeCdrRecord.dealer_msisdn);
        ps.setInt(5,rechargeCdrRecord.dealer_id);
        ps.setInt(6,rechargeCdrRecord.dealer_province);
        ps.setInt(7, rechargeCdrRecord.dealer_category);
        ps.setInt(8,rechargeCdrRecord.balance_changed_amount);
        ps.setLong(9,rechargeCdrRecord.balance_before);
        ps.setLong(10,rechargeCdrRecord.balance_after);
        ps.setString(11,rechargeCdrRecord.receiver_msidn);
        ps.setInt(12, rechargeCdrRecord.receiver_province);
        ps.setInt(13,rechargeCdrRecord.receiver_sub_type);
        ps.setDate(14, rechargeCdrRecord.receiver_active_date);
        ps.setDate(15, rechargeCdrRecord.receiver_new_expire_date);
        ps.setInt(16,rechargeCdrRecord.recharge_value);
        
        ps.setInt(17,rechargeCdrRecord.bonus_balance);
        ps.setInt(18,rechargeCdrRecord.bonus_data);
        ps.setInt(19,rechargeCdrRecord.promotion_balance_id);
        ps.setInt(20,rechargeCdrRecord.promotion_data_id);
        
        ps.setInt(21,rechargeCdrRecord.transaction_id);
        ps.setInt(22,rechargeCdrRecord.result);
        ps.setInt(23,rechargeCdrRecord.result_code);
        ps.setString(24,rechargeCdrRecord.result_description);
        ps.execute();
        ps.close();
    }

	public void moveDealerProvince(MoveDealerProvinceCmd moveDealerProvinceCmd) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "{call move_dealer_province (?, ?, ?, ?, ?, ?, ?)}";
		CallableStatement stmt = null;
		stmt = connection.prepareCall(sql);
		stmt.setInt(1, moveDealerProvinceCmd.dealer_id);
		stmt.setInt(2, moveDealerProvinceCmd.new_provice_code);
		stmt.setInt(3, moveDealerProvinceCmd.new_customer_care);
		stmt.setString(4, moveDealerProvinceCmd.approved);
		stmt.setInt(5, moveDealerProvinceCmd.approved_id);
		stmt.registerOutParameter(6, java.sql.Types.INTEGER);
		stmt.registerOutParameter(7, java.sql.Types.INTEGER);
		stmt.execute();
		moveDealerProvinceCmd.return_code = stmt.getInt(6);
		moveDealerProvinceCmd.new_dealer_id = stmt.getInt(7);
		stmt.close();
	}
	
	public Hashtable<String, Province> loadProvinces() throws SQLException {
		// TODO Auto-generated method stub
		Hashtable<String, Province> provinces = new Hashtable<String, Province>();
		PreparedStatement ps=connection.prepareStatement(
				"SELECT msisdn_prefix, provice_code FROM msisdn_prefix");
		ps.execute();
		ResultSet rs = ps.getResultSet();

		while(rs.next()) {
			Province province = new Province();
			province.msisdn_prefix = rs.getString("msisdn_prefix");
			province.province_code = rs.getInt("provice_code");
			provinces.put(province.msisdn_prefix, province);
			int lenght = province.msisdn_prefix.length();
			if(Province.MAX_MSISDN_PREFIX_LENGTH < lenght)
				Province.MAX_MSISDN_PREFIX_LENGTH = lenght;
		}
		rs.close();
		ps.close();
		
		return provinces;
	}

	public boolean isDealerHasRetailer(int dealer_id) throws SQLException {
		// TODO Auto-generated method stub
		boolean result = false;
		PreparedStatement ps=connection.prepareStatement(
				"select 1 from dealers where active IN(1,2,3) AND parent_id = ? limit 1");
		ps.setInt(1, dealer_id);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			result = true;
		}
		rs.close();
		ps.close();
		return result;
	}
	
	public Promotion getPromotionBalance(int topupAmount) throws SQLException {
		// TODO Auto-generated method stub
		Promotion promotion = null;
		PreparedStatement ps=connection.prepareStatement(
				"SELECT id, date_time, agent_init_id, from_date, to_date, topup_value_level, param_type, param_value FROM promotions WHERE promotion_type = ? AND from_date<=DATE(NOW()) AND to_date>=DATE(NOW()) AND topup_value_level <=? AND status = ? ORDER BY topup_value_level DESC limit 1");
		ps.setInt(1, Promotion.PROMOTION_TYPE_BALANCE);
		ps.setInt(2, topupAmount);
		ps.setInt(3, Promotion.STATUS_ACTIVE);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			promotion = new Promotion();
			promotion.id = rs.getInt("id");
			promotion.promotion_type = Promotion.PROMOTION_TYPE_BALANCE;
			promotion.agent_init_id = rs.getInt("agent_init_id");
			promotion.date_time = rs.getTimestamp("date_time");
			promotion.from_date = rs.getDate("from_date");
			promotion.to_date = rs.getDate("to_date");
			promotion.topup_value_level = rs.getInt("topup_value_level");
			promotion.param_type = rs.getInt("param_type");
			promotion.param_value = rs.getDouble("param_value");
		}
		rs.close();
		ps.close();
		return promotion;
	}
	
	public Promotion getPromotionData(int topupAmount) throws SQLException {
		// TODO Auto-generated method stub
		Promotion promotion = null;
		PreparedStatement ps=connection.prepareStatement(
				"SELECT id, date_time, agent_init_id, from_date, to_date, topup_value_level, param_type, param_value FROM promotions WHERE promotion_type = ? AND from_date<=DATE(NOW()) AND to_date>=DATE(NOW()) AND topup_value_level <=? AND status = ? ORDER BY topup_value_level DESC limit 1");
		ps.setInt(1, Promotion.PROMOTION_TYPE_DATA);
		ps.setInt(2, topupAmount);
		ps.setInt(3, Promotion.STATUS_ACTIVE);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			promotion = new Promotion();
			promotion.id = rs.getInt("id");
			promotion.promotion_type = Promotion.PROMOTION_TYPE_DATA;
			promotion.agent_init_id = rs.getInt("agent_init_id");
			promotion.date_time = rs.getTimestamp("date_time");
			promotion.from_date = rs.getDate("from_date");
			promotion.to_date = rs.getDate("to_date");
			promotion.topup_value_level = rs.getInt("topup_value_level");
			promotion.param_type = rs.getInt("param_type");
			promotion.param_value = rs.getDouble("param_value");
		}
		rs.close();
		ps.close();
		return promotion;
	}
}
