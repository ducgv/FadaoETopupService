package com.itpro.etopup.struct;

import java.io.UnsupportedEncodingException;
import java.util.Vector;

import com.itpro.smsbuilder.Dcs;
import com.itpro.smsbuilder.SmsBuilder;
import com.itpro.smsbuilder.SmsContent;
import com.logica.smpp.util.ByteBuffer;
import com.logica.smpp.util.NotEnoughDataInByteBufferException;

public class MTRecord {
	public int id;
	public String msisdn;
	public int sms_type; //0: mca, 1: auto_reply, 2: notify_me
	public byte[] content;
	public String service_code = "";
	public String message_id = "";
	public int status;
	public int err_code = 0;
	public int retry;	
	public Vector<SmsContent> lstContent;
	public byte[] partData;
	public int numPart;
	public int dcs;
	public int udhi;
	public int curIndex = 0;
	public int builded;
	public String tblName;
	public long sendTime;
	public int transaction_id = 0;
	
	public MTRecord(){
		
	}
	public MTRecord(String msisdn, String content, int sms_type, int transaction_id){
		this.msisdn = msisdn;
		try {
			this.content = content.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			this.content = content.getBytes();
		}
		this.sms_type = sms_type;
		this.transaction_id = transaction_id;
	}
	
	public static boolean isAscii(String content) {
		for(int i=0;i<content.length();i++)
			if(content.charAt(i)>=0x80)
				return false;
		return true;
	}
	
	public void AnalizePartData(){
		ByteBuffer buff = new ByteBuffer();
		buff.appendBytes(partData);
		lstContent = new Vector<SmsContent>();
		while (buff.length()>0) {
			SmsContent element = new SmsContent();
			element.bLongMessage = numPart>1?true:false;
			element.bUdhi = udhi!=0?true:false;
			element.dcs = dcs;			
			short len;
			try {
				len = buff.removeShort();
				element.content = buff.removeBytes(len).getBuffer();
				lstContent.add(element);
			} catch (NotEnoughDataInByteBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void AnalizeContent(){	
		try {
			curIndex = 0;
			if(isAscii(new String(content))) {
				lstContent = SmsBuilder.SplitContentForSMS(null, new String(content).getBytes(), Dcs.DCS_TEXT_ASCII);
				dcs = Dcs.DCS_TEXT_ASCII;
				numPart = lstContent.size();
				udhi = lstContent.firstElement().bUdhi?1:0;
			}
			else{
				byte[] tmp = (new String(content, "UTF-8")).getBytes("UTF-16");				
				byte[] ucs2Content = new byte[tmp.length-2];				
				System.arraycopy(tmp, 2, ucs2Content, 0, tmp.length-2);				
				lstContent = SmsBuilder.SplitContentForSMS(null, ucs2Content, Dcs.DCS_TEXT_UNICODE);
				dcs = Dcs.DCS_TEXT_UNICODE;
				numPart = lstContent.size();
				udhi = lstContent.firstElement().bUdhi?1:0;
			}
			ByteBuffer buff = new ByteBuffer();
			for(int i=0;i<lstContent.size();i++){
				buff.appendShort((short)lstContent.get(i).content.length);
				buff.appendBytes(lstContent.get(i).content);
				partData=buff.getBuffer();
			}

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			lstContent = new Vector<SmsContent>();
		}
	}
}
