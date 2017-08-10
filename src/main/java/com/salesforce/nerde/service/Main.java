package com.salesforce.nerde.service;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import com.salesforce.nerde.service.ResourceDiscoveryService.HttpMethod;

public class Main {

	public static void main(String[] args) {
		final ResourceDiscoveryService s= ResourceDiscoveryService.getInstance("shared2-argusmqzkdev1-1-prd.eng.sfdc.net:2181", "/argus/services/dev/tsdb/write", 200);
		new FedThread(s).start();
		new FedThread(s).start();
	}
}

class FedThread extends Thread{
	ResourceDiscoveryService s;
	public FedThread(ResourceDiscoveryService s){
		this.s=s;
	}
	@Override
	public void run(){
		while(true){
			try {
				Thread.sleep(2000);
				HttpResponse response = s.executeHttpRequest(HttpMethod.GET, "api/query?start=1502238&m=sum:alerts.scheduled-__-argus.core", null);
				EntityUtils.consumeQuietly(response.getEntity());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
