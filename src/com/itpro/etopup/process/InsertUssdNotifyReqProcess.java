/**
 * 
 */
package com.itpro.etopup.process;

import java.sql.SQLException;

import com.itpro.etopup.db.DbConnection;
import com.itpro.etopup.main.Config;
import com.itpro.etopup.struct.MTRecord;
import com.itpro.util.MySQLConnection;
import com.itpro.util.ProcessingThread;
import com.itpro.util.Queue;

/**
 * @author Giap Van Duc
 *
 */
public class InsertUssdNotifyReqProcess extends ProcessingThread {
	Queue queueInsertNotifyReq = new Queue();
	private DbConnection connection = null;
	public boolean isConnected = false;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.itpro.util.ProcessingThread#OnHeartBeat()
	 */
	@Override
	protected void OnHeartBeat() {
		// TODO Auto-generated method stub
		if (connection == null) {
			Connect();
		} else if (!isConnected) {
			connection.close();
			Connect();
		}
		if(isConnected){
			try {
				connection.checkConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				logError("Check DB connection error:"+e.getMessage());
				isConnected = false;
			}
		}
	}

	private void OnConnected() {

	}

	private void Connect() {
		connection = new DbConnection(Config.dbServerName, Config.dbDatabaseName, Config.dbUserName, Config.dbPassword);
		Exception exception = null;
		try {
			isConnected = connection.connect();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			exception = e;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			exception = e;
		}

		if (exception != null) {
			isConnected = false;
			logError("Connect to DB: error:" + exception.getMessage());
		} else {
			OnConnected();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.itpro.util.ProcessingThread#initialize()
	 */
	@Override
	protected void initialize() {
		// TODO Auto-generated method stub
		setHeartBeatInterval(5000);
		Connect();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.itpro.util.ProcessingThread#process()
	 */
	@Override
	protected void process() {
		// TODO Auto-generated method stub
		if(!isConnected)
			return;
		MTRecord mtRecord = (MTRecord) queueInsertNotifyReq.dequeue();
		if(mtRecord!=null){
			try {
				connection.insertUssdNotifyRecord(mtRecord);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logError("Insert " + mtRecord.toString()+ "; error:" + MySQLConnection.getSQLExceptionString(e));
				isConnected = false;
				queueInsertNotifyReq.enqueue(mtRecord);
			}
		}

	}
}
