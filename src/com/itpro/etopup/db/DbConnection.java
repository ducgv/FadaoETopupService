/**
 * 
 */
package com.itpro.etopup.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import com.itpro.etopup.main.Config;
import com.itpro.etopup.struct.AddBalanceRate;
import com.itpro.etopup.struct.AgentRequest;
import com.itpro.etopup.struct.CDRRecord;
import com.itpro.etopup.struct.DealerInfo;
import com.itpro.etopup.struct.DealerRequest;
import com.itpro.etopup.struct.MTRecord;
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
				"select id, msisdn, pin_code, account_balance from dealers where msisdn = ? and active IN (1,2,3)");
		ps.setString(1, msisdn);
		ps.execute();
		ResultSet rs = ps.getResultSet();
		if(rs.next()) {
			dealerInfo = new DealerInfo();
			dealerInfo.id = rs.getInt(1);
			dealerInfo.msisdn = rs.getString(2);
			dealerInfo.pin_code = rs.getString(3);
			dealerInfo.balance = rs.getInt(4);
		}
		rs.close();
		ps.close();
		return dealerInfo;
	}
	   public DealerInfo getDealerInfo(int dealerId) throws SQLException {
	        // TODO Auto-generated method stub
	        DealerInfo dealerInfo = null;
	        PreparedStatement ps=connection.prepareStatement(
	                "select id, msisdn, pin_code, account_balance from dealers where id = ? ");
	        ps.setInt(1, dealerId);
	        ps.execute();
	        ResultSet rs = ps.getResultSet();
	        if(rs.next()) {
	            dealerInfo = new DealerInfo();
	            dealerInfo.id = rs.getInt(1);
	            dealerInfo.msisdn = rs.getString(2);
	            dealerInfo.pin_code = rs.getString(3);
	            dealerInfo.balance = rs.getInt(4);
	        }
	        rs.close();
	        ps.close();
	        return dealerInfo;
	    }
    public TransactionRecord getTransactionRecord(int id) throws SQLException {
        // TODO Auto-generated method stub
        TransactionRecord transactionRecord = null;
        PreparedStatement ps=connection.prepareStatement(
                "select id, date_time, type, dealer_msisdn, dealer_id, balance_changed_amount, balance_before, balance_after, "
                    + "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, refund_status, result_description from transactions where id=?");
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
            transactionRecord.balance_changed_amount = rs.getInt("balance_changed_amount");
            transactionRecord.balance_before = rs.getInt("balance_before");
            transactionRecord.balance_after = rs.getInt("balance_after");
            transactionRecord.partner_msisdn = rs.getString("partner_msisdn");
            transactionRecord.partner_id = rs.getInt("partner_id");
            transactionRecord.partner_balance_before = rs.getInt("partner_balance_before");
            transactionRecord.partner_balance_after = rs.getInt("partner_balance_after");
            transactionRecord.status = rs.getInt("status");
            transactionRecord.refund_status = rs.getInt("refund_status");
            transactionRecord.result_description = rs.getString("result_description");
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
					+ "(id, date_time, type, dealer_msisdn, dealer_id, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "partner_msisdn, partner_id, partner_balance_before, partner_balance_after, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
			ps.setString(10, transactionRecord.partner_msisdn);
			ps.setInt(11, transactionRecord.partner_id);
			ps.setLong(12, transactionRecord.partner_balance_before);
			ps.setLong(13, transactionRecord.partner_balance_after);
			ps.setInt(14, transactionRecord.status);
			ps.setString(15, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_RECHARGE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "recharge_msidn, recharge_value, recharge_sub_type, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
			ps.setString(10, transactionRecord.recharge_msidn);
			ps.setInt(11, transactionRecord.recharge_value);
			ps.setInt(12, transactionRecord.recharge_sub_type);
			ps.setInt(13, transactionRecord.status);
			ps.setString(14, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_CREATE_ACCOUNT:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, cash_value, invoice_code, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
			ps.setLong(12, transactionRecord.cash_value);
			ps.setString(13, transactionRecord.invoice_code);
			ps.setInt(14, transactionRecord.status);
			ps.setString(15, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		case TransactionRecord.TRANS_TYPE_ADD_BALANCE:
			sql = "INSERT INTO transactions"
					+ "(id, date_time, type, dealer_msisdn, dealer_id, transaction_amount_req, balance_changed_amount, balance_before, balance_after, "
					+ "agent, agent_id, cash_value, invoice_code, status, result_description) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
			ps.setLong(12, transactionRecord.cash_value);
			ps.setString(13, transactionRecord.invoice_code);
			ps.setInt(14, transactionRecord.status);
			ps.setString(15, transactionRecord.result_description);
			ps.execute();
			ps.close();
			break;
		}
		
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
				"SELECT id, req_type, req_date, agent_username, agent_id, dealer_msisdn, dealer_name, dealer_id_card_number, "
				+ "dealer_birthdate, dealer_province, dealer_address, cash_value, invoice_code, "
				+ "refund_transaction_id FROM agent_requests WHERE status = 0");
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
			agentRequest.dealer_id_card_number = rs.getString("dealer_id_card_number");
			agentRequest.dealer_birthdate = rs.getDate("dealer_birthdate");

			try {
				byte[] dealer_province = rs.getBytes("dealer_province");
				if(dealer_province!=null)
					agentRequest.dealer_province = new String(dealer_province, "UTF-8");
				else 
					agentRequest.dealer_province = null;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				agentRequest.dealer_province = rs.getString("dealer_province");
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
			agentRequest.cash_value = rs.getInt("cash_value");
			agentRequest.invoice_code = rs.getString("invoice_code");
			agentRequest.refund_transaction_id = rs.getInt("refund_transaction_id");
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
	public Vector<AddBalanceRate> getConfigAddBalanceRate() throws SQLException{
		Vector<AddBalanceRate> addBalanceRates = new Vector<AddBalanceRate>();
		PreparedStatement ps=connection.prepareStatement(
				"SELECT cash_level, rate FROM config_add_balance_rate ORDER BY cash_level asc");
		ps.execute();
		ResultSet rs = ps.getResultSet();
		while(rs.next()) {
			AddBalanceRate addBalanceRate = new AddBalanceRate();
			addBalanceRate.cashLevel = rs.getLong("cash_level");
			addBalanceRate.rate = rs.getDouble("rate");
			addBalanceRates.add(addBalanceRate);
		}
		rs.close();
		ps.close();
		return addBalanceRates;
	}

	public void insertDealer(DealerInfo dealerInfo) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = null;		
		ps=connection.prepareStatement("INSERT INTO dealers ("
				+ "msisdn, pin_code, register_date, agent_approved, agent_approved_id, name, birth_date, id_card_number, province, address, "
				+ "account_balance, active) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
		ps.setString(1, dealerInfo.msisdn);
		ps.setString(2, dealerInfo.pin_code);
		ps.setTimestamp(3, dealerInfo.register_date);
		ps.setString(4, dealerInfo.agent_approved);
		ps.setInt(5, dealerInfo.agent_approved_id);
		try {
			if(dealerInfo.name!=null)
				ps.setBytes(6, dealerInfo.name.getBytes("UTF-8"));
			else
				ps.setString(6,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(6, dealerInfo.name);
		}
		ps.setDate(7, dealerInfo.birth_date);
		ps.setString(8, dealerInfo.id_card_number);
		try {
			if(dealerInfo.province!=null)
				ps.setBytes(9, dealerInfo.province.getBytes("UTF-8"));
			else
				ps.setString(9,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(9, dealerInfo.province);
		}
		try {
			if(dealerInfo.address!=null)
				ps.setBytes(10, dealerInfo.address.getBytes("UTF-8"));
			else
				ps.setString(10,null);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			ps.setString(10, dealerInfo.address);
		}
		ps.setLong(11, dealerInfo.balance);
		ps.setInt(12, dealerInfo.active);
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

	public void updateAgentRequest(AgentRequest agentRequest) throws SQLException {
		// TODO Auto-generated method stub
		String sql = "UPDATE agent_requests SET status=?, result_description=?";;
		if(agentRequest.dealer_id!=0)
			sql+=", dealer_id=?";
		if(agentRequest.transaction_id!=0)
			sql+=", transaction_id=?";
		sql+=", balance_add_amount=?";
		sql += " WHERE id=?";
		PreparedStatement ps = null;
		ps=connection.prepareStatement(sql);
		ps.setInt(1, agentRequest.status);
		ps.setString(2, agentRequest.result_description);
		int index = 2;
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

	public void deductBalance(BatchRechargeElement batchRechargeElement) throws SQLException{
		// TODO Auto-generated method stub
		
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
}
