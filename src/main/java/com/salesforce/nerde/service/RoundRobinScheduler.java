/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of Salesforce.com nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.nerde.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.swing.event.ChangeListener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.nerde.exception.NoHostException;
/**
 * Maintains a connection to ZooKeeper, refreshes hosts periodically and provides methods to read host info, delete hosts etc. 
 * @author  Raj Sarkapally (rsarkapally@salesforce.com)
 *
 */
public class RoundRobinScheduler {
	private final Logger _logger=LoggerFactory.getLogger(RoundRobinScheduler.class);

	String zkURL, path;
	List<Host> hosts;
	int currentHostIndex;
	Object lock=new Object();
	boolean updateInProgress;
	CuratorFramework client;
	ObjectMapper mapper;
	ChangeListener listener;

	public RoundRobinScheduler(String zkURL, String path, ChangeListener listerner){
		this.zkURL=zkURL;
		this.path=path;
		this.listener=listerner;
		currentHostIndex=0;
		updateInProgress=true;
		mapper=new ObjectMapper();
		initializeCuratorFramework();
		updateHosts();
	}

	private void initializeCuratorFramework(){
		client = CuratorFrameworkFactory.newClient(zkURL,Integer.MAX_VALUE,Integer.MAX_VALUE, new ExponentialBackoffRetry(1000, 29));
		client.start();
		try{
			if(client.checkExists().forPath(path) ==null){
				client.create().creatingParentContainersIfNeeded().forPath(path,new byte[0]); 
			}
		}catch(Exception e){
			//TODO: throw exception instead of handling here
		}

		client.getCuratorListenable().addListener(new CuratorListener() {
			public void eventReceived(CuratorFramework framework, CuratorEvent event) throws Exception {
				listener.stateChanged(null);
			}
		});
	}
//TODO: Make nerdeHost as generic and use Interface Host
	public void updateHosts(){
		_logger.info("Updating hosts started");
		hosts=new ArrayList<Host>();
		synchronized(lock){
			try {
				updateInProgress=true;
				List<String> children = client.getChildren().watched().forPath(path);
				for(String child:children){
					try{
						String nodeData=new String(client.getData().forPath(path+"/"+child));
						if(nodeData !=null && nodeData.length()>0){
							NerveHost host = mapper.readValue(nodeData, NerveHost.class); 
							hosts.add(host);
						}

					}catch (Throwable e) {
						_logger.info("Failed to process node: " + new String(client.getData().forPath(path+"/"+child)) + "Reason: " + e.getMessage());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			updateInProgress=false;
			lock.notifyAll();
		}
		_logger.info("Updating hosts complete");
	}

	public Host getHost() throws NoHostException{ 
		checkIfUpdateInProgress();
		synchronized(this){//TODO: for more performance, remove synchronized
			currentHostIndex++;
			if(currentHostIndex>=hosts.size()){
				currentHostIndex=0;
			}
			if(hosts.size()>0){
				return hosts.get(currentHostIndex);
			}else{
				throw new NoHostException("No host is available");
			}
		}
	}
	public void deleteNode(String nodeName) throws Exception{
		checkIfUpdateInProgress();
		client.delete().forPath(path+"/"+nodeName);
		_logger.info("deleted host: {}", nodeName);
	}
	public List<Host> getAllHosts(){
		checkIfUpdateInProgress();
		return hosts;
	}
	public void dispose(){
		client.close();
	}
	private void checkIfUpdateInProgress(){
		if(updateInProgress){
			try {
				synchronized(lock){
					if(updateInProgress){
						_logger.info("Updating hosts is in progress so the current thread {} will wait until the update is complete", Thread.currentThread().getName()); 
						lock.wait();
					}
				}
			} catch (InterruptedException e) {
				_logger.error(e.getMessage());
			}
		}
	}

	/*
	 * This will update every hour and make sure that session will not expire
	 */
	private void updateFrequntly(){
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				_logger.info("Frequent update is starting.");
				updateHosts();
			}
		}, 1, 1, TimeUnit.HOURS);
	}
}
