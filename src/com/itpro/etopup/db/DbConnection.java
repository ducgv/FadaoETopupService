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
import com.itpro.etopup.struct.CDRRecord;
import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;
import com.itpro.etopup.struct.MTRecord;
import com.itpro.etopup.struct.MoveDealerProvinceCmd;
import com.itpro.etopup.struct.Province;
import com.itpro.etopup.struct.RechargeCdrRecord;
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
				"select id, msisdn, pin_code, web_password, province_register, account_balance, parent_id, category from dealers where msisdn = ? and active IN (1,2,3)");
		ps.setString(1, msisdn);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			dealerInfo = new DealerInfo();
			dealerInfo.id = rs.getInt("id");
			dealerInfo.msisdn = rs.getString("msisdn");
			dealerInfo.pin_code = rs.getString("pin_code");
			dealerInfo.web_password = rs.getString("web_password");
			dealerInfo.province_register = rs.getInt("province_register");
			dealerInfo.balance = rs.getInt("account_balance");
			dealerInfo.parent_id = rs.getInt("parent_id");
			dealerInfo.category = rs.getInt("category");
		}
		rs.close();
		ps.close();
		return dealerInfo;
	}
	
	public DealerInfo getDealerInfo(int dealerId) throws SQLException {
		// TODO Auto-generated method stub
		DealerInfo dealerInfo = null;
		PreparedStatement ps=connection.prepareStatement(
				"select id, msisdn, pin_code, province_register, account_balance, parent_id, category from dealers where id = ? ");
		ps.setInt(1, dealerId);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			dealerInfo = new DealerInfo();
			dealerInfo.id = rs.getInt("id");
			dealerInfo.msisdn = rs.getString("msisdn");
			dealerInfo.pin_code = rs.getString("pin_code");
			dealerInfo.province_register = rs.getInt("province_register");
			dealerInfo.balance = rs.getInt("account_balance");
			dealerInfo.parent_id = rs.getInt("parent_id");
			dealerInfo.category = rs.getInt("category");
		}
		rs.close();
		ps.close();
		return dealerInfo;
	}
	
	public AgentInfo getAgentInfo(int agentId) throws SQLException {
		// TODO Auto-generated method stub
		AgentInfo agentInfo = null;
		PreparedStatement ps=connection.prepareStatement(
				"select username, province_code from users where id = ? ");
		ps.setInt(1, agentId);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			agentInfo = new AgentInfo();
			agentInfo.id = agentId;
			agentInfo.user_name = rs.getString(1);
			agentInfo.province_code = rs.getInt(2);
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
                    + "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, refund_status, result_description,recharge_msidn,recharge_value,batch_recharge_id from transactions where id=?");
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
		case TransactionRecord.TRANS_TYPE_MOVE_STOCK:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.partner_msisdn);
			ps.setInt(12, transactionRecord.partner_id);
			ps.setLong(13, transactionRecord.partner_balance_before);
			ps.setLong(14, transactionRecord.partner_balance_after);
			ps.setInt(15, transactionRecord.status);
			ps.setString(16, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_RECHARGE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.recharge_msidn);
			ps.setInt(12, transactionRecord.recharge_value);
			ps.setInt(13, transactionRecord.recharge_sub_type);
			ps.setInt(14, transactionRecord.status);
			ps.setString(15, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_BATCH_RECHARGE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description,`batch_recharge_id`,`batch_recharge_succes`,`batch_recharge_fail`) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.recharge_msidn);
			ps.setInt(12, transactionRecord.recharge_value);
			ps.setInt(13, transactionRecord.recharge_sub_type);
			ps.setInt(14, transactionRecord.status);
			ps.setString(15, transactionRecord.result_description);
			ps.setInt(16, transactionRecord.batch_recharge_id);
			ps.setInt(17, transactionRecord.batch_recharge_succes);
			ps.setInt(18, transactionRecord.batch_recharge_fail);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_CREATE_DEALER:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, cash_value, commision_value, iv_cash_kip, iv_cash_baht, iv_cash_usd, iv_cash_yuan, iv_rate_baht, iv_rate_usd, iv_rate_yuan, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.agent);
			ps.setInt(12, transactionRecord.agent_id);
			ps.setString(13, transactionRecord.approved);
			ps.setInt(14, transactionRecord.approved_id);
			ps.setLong(15, transactionRecord.addBalanceInfo.cash_value);
			ps.setLong(16, transactionRecord.addBalanceInfo.commision_value);
			ps.setLong(17, transactionRecord.addBalanceInfo.iv_cash_kip);
			ps.setLong(18, transactionRecord.addBalanceInfo.iv_cash_baht);
			ps.setLong(19, transactionRecord.addBalanceInfo.iv_cash_usd);
			ps.setLong(20, transactionRecord.addBalanceInfo.iv_cash_yuan);
			ps.setLong(21, transactionRecord.addBalanceInfo.iv_rate_baht);
			ps.setLong(22, transactionRecord.addBalanceInfo.iv_rate_usd);
			ps.setLong(23, transactionRecord.addBalanceInfo.iv_rate_yuan);
			ps.setInt(24, transactionRecord.status);
			ps.setString(25, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_CREATE_SUB_DEALER:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_parent_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_parent_id);
			ps.setInt(7, transactionRecord.dealer_province);
			ps.setLong(8, transactionRecord.transaction_amount_req);
			ps.setLong(9, transactionRecord.balance_changed_amount);
			ps.setLong(10, transactionRecord.balance_before);
			ps.setLong(11, transactionRecord.balance_after);
			ps.setString(12, transactionRecord.agent);
			ps.setInt(13, transactionRecord.agent_id);
			ps.setString(14, transactionRecord.approved);
			ps.setInt(15, transactionRecord.approved_id);
			ps.setInt(16, transactionRecord.status);
			ps.setString(17, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_ADD_BALANCE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, cash_value, commision_value, iv_cash_kip, iv_cash_baht, iv_cash_usd, iv_cash_yuan, iv_rate_baht, iv_rate_usd, iv_rate_yuan, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.agent);
			ps.setInt(12, transactionRecord.agent_id);
			ps.setString(13, transactionRecord.approved);
			ps.setInt(14, transactionRecord.approved_id);
			ps.setLong(15, transactionRecord.addBalanceInfo.cash_value);
			ps.setLong(16, transactionRecord.addBalanceInfo.commision_value);
			ps.setLong(17, transactionRecord.addBalanceInfo.iv_cash_kip);
			ps.setLong(18, transactionRecord.addBalanceInfo.iv_cash_baht);
			ps.setLong(19, transactionRecord.addBalanceInfo.iv_cash_usd);
			ps.setLong(20, transactionRecord.addBalanceInfo.iv_cash_yuan);
			ps.setLong(21, transactionRecord.addBalanceInfo.iv_rate_baht);
			ps.setLong(22, transactionRecord.addBalanceInfo.iv_rate_usd);
			ps.setLong(23, transactionRecord.addBalanceInfo.iv_rate_yuan);
			ps.setInt(24, transactionRecord.status);
			ps.setString(25, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_REFUND_RECHARGE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description,agent,agent_id,approved, approved_id,refund_transaction_id,`batch_recharge_id`) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.recharge_msidn);
			ps.setInt(12, transactionRecord.recharge_value);
			ps.setInt(13, transactionRecord.recharge_sub_type);
			ps.setInt(14, transactionRecord.status);
			ps.setString(15, transactionRecord.result_description);
			ps.setString(16, transactionRecord.agent);
			ps.setInt(17, transactionRecord.agent_id);
			ps.setString(18, transactionRecord.approved);
			ps.setInt(19, transactionRecord.approved_id);
			ps.setInt(20, transactionRecord.refund_transaction_id);
			ps.setInt(21, transactionRecord.batch_recharge_id);
			ps.execute();
			ps.close();
			break;	
		case TransactionRecord.TRANS_TYPE_REFUND_MOVE_STOCK:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, result_description,agent,agent_id,approved, approved_id,refund_transaction_id) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.partner_msisdn);
			ps.setInt(12, transactionRecord.partner_id);
			ps.setLong(13, transactionRecord.partner_balance_before);
			ps.setLong(14, transactionRecord.partner_balance_after);
			ps.setInt(15, transactionRecord.status);
			ps.setString(16, transactionRecord.result_description);
			ps.setString(17, transactionRecord.agent);
			ps.setInt(18, transactionRecord.agent_id);
			ps.setString(19, transactionRecord.approved);
			ps.setInt(20, transactionRecord.approved_id);
			ps.setInt(21, transactionRecord.refund_transaction_id);
			ps.execute();
			ps.close();
			break;  
		case TransactionRecord.TRANS_TYPE_REFUND_ADD_BALANCE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, dealer_province, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setInt(6, transactionRecord.dealer_province);
			ps.setLong(7, transactionRecord.transaction_amount_req);
			ps.setLong(8, transactionRecord.balance_changed_amount);
			ps.setLong(9, transactionRecord.balance_before);
			ps.setLong(10, transactionRecord.balance_after);
			ps.setString(11, transactionRecord.agent);
			ps.setInt(12, transactionRecord.agent_id);
			ps.setString(13, transactionRecord.approved);
			ps.setInt(14, transactionRecord.approved_id);
			ps.setInt(15, transactionRecord.status);
			ps.setString(16, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_MOVE_DEALER_OLD_PROVINCE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, dealer_province, dealer_new_id, dealer_new_province, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setLong(6, transactionRecord.transaction_amount_req);
			ps.setLong(7, transactionRecord.balance_changed_amount);
			ps.setLong(8, transactionRecord.balance_before);
			ps.setLong(9, transactionRecord.balance_after);
			ps.setString(10, transactionRecord.agent);
			ps.setInt(11, transactionRecord.agent_id);
			ps.setString(12, transactionRecord.approved);
			ps.setInt(13, transactionRecord.approved_id);
			ps.setInt(14, transactionRecord.dealer_province);
			ps.setInt(15, transactionRecord.dealer_new_id);
			ps.setInt(16, transactionRecord.dealer_new_province);
			ps.setInt(17, transactionRecord.status);
			ps.setString(18, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break; 	
		case TransactionRecord.TRANS_TYPE_MOVE_DEALER_NEW_PROVINCE_ACCEPTED:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, approved, approved_id, dealer_province, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql);
			ps.setInt(1, transactionRecord.id);
			ps.setTimestamp(2, transactionRecord.date_time);
			ps.setInt(3, transactionRecord.type);
			ps.setString(4, transactionRecord.dealer_msisdn);
			ps.setInt(5, transactionRecord.dealer_id);
			ps.setLong(6, transactionRecord.transaction_amount_req);
			ps.setLong(7, transactionRecord.balance_changed_amount);
			ps.setLong(8, transactionRecord.balance_before);
			ps.setLong(9, transactionRecord.balance_after);
			ps.setString(10, transactionRecord.agent);
			ps.setInt(11, transactionRecord.agent_id);
			ps.setString(12, transactionRecord.approved);
			ps.setInt(13, transactionRecord.approved_id);
			ps.setInt(14, transactionRecord.dealer_province);
			ps.setInt(15, transactionRecord.status);
			ps.setString(16, transactionRecord.result_description);
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
				"SELECT id, req_type, req_date, agent_username, agent_id, approve_id, dealer_msisdn, dealer_name, dealer_parent_id, dealer_province_code, dealer_id_card_number, "
				+ "dealer_birthdate, dealer_address, cash_value, commision_value, iv_cash_kip, iv_cash_baht, iv_cash_usd, iv_cash_yuan, iv_rate_baht, iv_rate_usd, iv_rate_yuan, "
				+ "refund_transaction_id,refund_msisdn,refund_amount,web_password,category FROM agent_requests WHERE status = 0");
		ps.setMaxRows(30);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		while(rs.next()) {
			AgentRequest agentRequest = new AgentRequest();
			agentRequest.id = rs.getInt("id");
			agentRequest.req_type = rs.getInt("req_type");
			agentRequest.req_date = rs.getTimestamp("req_date");
			agentRequest.agent_username = rs.getString("agent_username");
			agentRequest.agent_id = rs.getInt("agent_id");
			agentRequest.agent_approved_id = rs.getInt("approve_id");
			agentRequest.dealer_msisdn = rs.getString("dealer_msisdn");
			
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
			agentRequest.option_dealer_province_code = rs.getInt("dealer_province_code");
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
			agentRequest.addBalanceInfo.commision_value = rs.getInt("commision_value");
			
			agentRequest.addBalanceInfo.iv_cash_kip = rs.getInt("iv_cash_kip");
			agentRequest.addBalanceInfo.iv_cash_baht = rs.getInt("iv_cash_baht");
			agentRequest.addBalanceInfo.iv_cash_usd = rs.getInt("iv_cash_usd");
			agentRequest.addBalanceInfo.iv_cash_yuan = rs.getInt("iv_cash_yuan");
			agentRequest.addBalanceInfo.iv_rate_baht = rs.getInt("iv_rate_baht");
			agentRequest.addBalanceInfo.iv_rate_usd = rs.getInt("iv_rate_usd");
			agentRequest.addBalanceInfo.iv_rate_yuan = rs.getInt("iv_rate_yuan");
			
			agentRequest.refund_transaction_id = rs.getInt("refund_transaction_id");
			agentRequest.refund_msisdn=rs.getString("refund_msisdn");
			agentRequest.refund_amount=rs.getInt("refund_amount");
			agentRequest.status = 0;
			agentRequest.web_password=rs.getString("web_password");
			agentRequest.category=rs.getInt("category");
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
				+ "msisdn, pin_code, register_date, agent_init, agent_init_id, agent_approved, agent_approved_id, name, parent_id, birth_date, id_card_number, province_register, address, "
				+ "account_balance, active,web_password,category) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
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
		ps.setInt(9, dealerInfo.parent_id);
		ps.setDate(10, dealerInfo.birth_date);
		ps.setString(11, dealerInfo.id_card_number);
		ps.setInt(12, dealerInfo.province_register);
		try {
			if(dealerInfo.address!=null)
				ps.setBytes(13, dealerInfo.address.getBytes("UTF-8"));
			else
				ps.setString(13,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(13, dealerInfo.address);
		}
		ps.setLong(14, dealerInfo.balance);
		ps.setInt(15, dealerInfo.active);
	    ps.setString(16, dealerInfo.web_password);
	    ps.setInt(17, dealerInfo.category);
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

	public void deleteDealer(DealerInfo dealerInfo) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("UPDATE dealers SET account_balance = ?, active = ? WHERE id = ?");
		ps.setLong(1, dealerInfo.balance);
		ps.setInt(2,DealerInfo.STATUS_DELETED);
		ps.setInt(3,dealerInfo.id);
		ps.executeUpdate();
		ps.close();
	}
	
	public void updateAgentRequest(AgentRequest agentRequest) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "UPDATE agent_requests SET status=?, result_code = ?, result_description=?";;
		if(agentRequest.dealer_id!=0)
			sql+=", dealer_id=?";
		if(agentRequest.transaction_id!=0)
			sql+=", transaction_id=?";
		sql+=", balance_add_amount=?";
		sql += " WHERE id=?";
		PreparedStatement ps = null;
		ps=connection.prepareStatement(sql);
		ps.setInt(1, agentRequest.status);
		ps.setInt(2, agentRequest.result_code);
		ps.setString(3, AgentRequest.resultString[agentRequest.result_code]);
		int index = 3;
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


    public void insertRefundCDRRecord(String msisdn,long charge_value,int result_code,String result_string,int status,int transactionID, String spID,String serviceID, int transactionRecordId) throws SQLException {
        // TODO Auto-generated method stub
        PreparedStatement ps = null;
        String sql = "INSERT `refund_cdr`(`date_time`,`msisdn`,`charge_value`,`result_code`,`result_string`,`status`,`transactionID`,`spID`,`serviceID`,`transactionRecordId`)"
                + " VALUES (now(),?,?,?,?,?,?,?,?,?)";
        ps = connection.prepareStatement(sql);
        ps.setString(1, msisdn);
        ps.setLong(2, charge_value);
        ps.setInt(3, result_code);
        ps.setString(4, result_string);
        ps.setInt(5, status);
        ps.setInt(6, transactionID);
        ps.setString(7, spID);
        ps.setString(8, serviceID);
        ps.setInt(9, transactionRecordId);
        ps.execute();
        ps.close();
    }
    public void insertRecharge_cdr(RechargeCdrRecord rechargeCdrRecord) throws SQLException {
        // TODO Auto-generated method stub
        PreparedStatement ps = null;
        String sql = "INSERT INTO `recharge_cdr`(`payment_transaction_id`,`date_time`,`type`,`dealer_msisdn`,`dealer_id`,`dealer_province`,`dealer_category`,`balance_changed_amount`,`balance_before`,`balance_after`,`receiver_msidn`,`receiver_province`,`receiver_sub_type`,`recharge_value`,`receiver_balance_before`,`receiver_balance_after`,`transaction_id`,`result`,`result_code`,`result_description`) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        ps=connection.prepareStatement(sql);
        ps.setInt(1,rechargeCdrRecord.payment_transaction_id);
        ps.setTimestamp(2,rechargeCdrRecord.date_time);
        ps.setInt(3,rechargeCdrRecord.type);
        ps.setString(4,rechargeCdrRecord.dealer_msisdn);
        ps.setInt(5,rechargeCdrRecord.dealer_id);
        ps.setInt(6, rechargeCdrRecord.dealer_province);
        ps.setInt(7, rechargeCdrRecord.dealer_category);
        ps.setInt(8,rechargeCdrRecord.balance_changed_amount);
        ps.setLong(9,rechargeCdrRecord.balance_before);
        ps.setLong(10,rechargeCdrRecord.balance_after);
        ps.setString(11,rechargeCdrRecord.receiver_msidn);
        ps.setInt(12, rechargeCdrRecord.receiver_province);
        ps.setInt(13,rechargeCdrRecord.receiver_sub_type);
        ps.setInt(14,rechargeCdrRecord.recharge_value);
        ps.setInt(15,rechargeCdrRecord.receiver_balance_before);
        ps.setInt(16,rechargeCdrRecord.receiver_balance_after);
        ps.setInt(17,rechargeCdrRecord.transaction_id);
        ps.setInt(18,rechargeCdrRecord.result);
        ps.setInt(19,rechargeCdrRecord.result_code);
        ps.setString(20,rechargeCdrRecord.result_description);
        ps.execute();
        ps.close();
    }
    
	public void insertCDRRecord(CDRRecord cdrRecord) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;
		String sql = "INSERT INTO cdr_topup(date_time,msisdn,province_code,sub_id,offer_id,offer_type,package_name,package_value,"
				+ "service_fee,add_amount,result_code,result_string,status, transactionID, spID, token) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		ps=connection.prepareStatement(sql);
		ps.setTimestamp(1,cdrRecord.date_time);
		ps.setString(2,cdrRecord.msisdn);
		ps.setInt(3,cdrRecord.province_code);
		ps.setString(4,cdrRecord.sub_id);
		ps.setInt(5,cdrRecord.offer_id);
		ps.setInt(6,cdrRecord.offer_type);
		ps.setString(7,cdrRecord.package_name);
		ps.setInt(8,cdrRecord.package_value);
		ps.setInt(9,cdrRecord.service_fee);
		ps.setInt(10,cdrRecord.add_amount);
		ps.setInt(11,cdrRecord.result_code);
		ps.setString(12,cdrRecord.result_string);
		ps.setInt(13,cdrRecord.status);	
		ps.setInt(14,cdrRecord.transactionID);
		ps.setString(15,cdrRecord.spID);
		ps.setString(16,cdrRecord.token);
		ps.execute();
		ps.close();
	}

	public void moveDealerProvince(MoveDealerProvinceCmd moveDealerProvinceCmd) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "{call move_dealer_province (?, ?, ?, ?, ?, ?)}";
		CallableStatement stmt = null;
		stmt = connection.prepareCall(sql);
		stmt.setInt(1, moveDealerProvinceCmd.dealer_id);
		stmt.setInt(2, moveDealerProvinceCmd.new_provice_code);
		stmt.setString(3, moveDealerProvinceCmd.approved);
		stmt.setInt(4, moveDealerProvinceCmd.approved_id);
		stmt.registerOutParameter(5, java.sql.Types.INTEGER);
		stmt.registerOutParameter(6, java.sql.Types.INTEGER);
		stmt.execute();
		moveDealerProvinceCmd.return_code = stmt.getInt(5);
		moveDealerProvinceCmd.new_dealer_id = stmt.getInt(6);
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
}
