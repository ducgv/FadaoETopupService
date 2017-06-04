/**
 * 
 */
package com.itpro.etopup.cli;

import com.itpro.cli.CmdReqProcess;
import com.itpro.cli.CmdRequest;
import com.itpro.etopup.main.GlobalVars;
/**
 * @author Giap Van Duc
 *
 */
public class ETopupCli extends CmdReqProcess {

	/* (non-Javadoc)
	 * @see com.itpro.cli.CmdReqProcess#OnHeartBeat()
	 */
	@Override
	protected void OnHeartBeat() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see com.itpro.cli.CmdReqProcess#OnRequest(com.itpro.cli.CmdRequest)
	 */
	@Override
	protected void OnRequest(CmdRequest cmdRequest) {
		// TODO Auto-generated method stub
		if(cmdRequest.cmd.equalsIgnoreCase("Stop")){
			String target = cmdRequest.params.get("module");
			if(target!=null){
				if(target.equalsIgnoreCase(CLICmd.MODULE_ETOPUP_SERVICE)){
					GlobalVars.stopModuleFlag = true;
					cmdRequest.result.put("Result", "success");
					cmdRequest.queueResp.enqueue(cmdRequest);			
				}
				else{
					cmdRequest.result.put("Result", "failed");
					cmdRequest.result.put("Error", "Syntax error");
					cmdRequest.queueResp.enqueue(cmdRequest);
				}
			}
			else{
				cmdRequest.result.put("Result", "failed");
				cmdRequest.result.put("Error", "Syntax error");
				cmdRequest.queueResp.enqueue(cmdRequest);
			}
		}
		else{			
			cmdRequest.result.put("Result", "failed");
			cmdRequest.result.put("Error", "Syntax error");
			cmdRequest.queueResp.enqueue(cmdRequest);			
		}
	}

}
