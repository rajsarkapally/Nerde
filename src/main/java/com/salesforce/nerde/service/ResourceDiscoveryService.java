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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.nerde.exception.NoHostException;

/**
 * Provides methods to read hosts, delete hosts. It maintains one instance for each service path 
 * @author  Raj Sarkapally (rsarkapally@salesforce.com)
 *
 */
public class ResourceDiscoveryService implements ChangeListener{
	private static Map<String, ResourceDiscoveryService> discoveryServiceMap=new HashMap<String, ResourceDiscoveryService>();
	private static int DEFAULT_CONNECTION_TIMEOUT=10000;
	private static int DEFAULT_SOCKET_TIMEOUT=300000;
	private int connectionCount=200; 
	private RoundRobinScheduler scheduler;
	private CloseableHttpClient httpClient;
	Object lock;
	boolean updateInProgress;
	private String DEFAULT_PROTOCOL="http://"; 
	private final Logger _logger=LoggerFactory.getLogger(ResourceDiscoveryService.class);
	private ResourceDiscoveryService(String zkURL, String servicePath, int connCount){
		if(connCount>0){
			connectionCount=connCount;
		}else{
			_logger.warn("Connection count must be atleast one. so setting connection count to default value {}",connCount); 
		}
		lock=new Object();
		updateInProgress=true;
		this.scheduler=new RoundRobinScheduler(zkURL, servicePath, this);
		httpClient=createHttpClient(connectionCount, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, this.getAllHosts());
		updateInProgress=false;
	};

	public static ResourceDiscoveryService getInstance(String zkURL, String servicePath, int connCount){
		if(discoveryServiceMap.containsKey(zkURL)){
			return discoveryServiceMap.get(zkURL);
		}else{
			synchronized (ResourceDiscoveryService.class) {
				if(!discoveryServiceMap.containsKey(zkURL)){
					ResourceDiscoveryService discoveryService =new ResourceDiscoveryService(zkURL,servicePath,connCount);
					discoveryServiceMap.put(zkURL, discoveryService);
				}
				return discoveryServiceMap.get(zkURL);
			}
		}
	}

	public Host getHost(){

		Host result=null;
		try{
			result=scheduler.getHost();
		}catch(NoHostException e){
			synchronized (lock) {
				try {
					if(getAllHosts().size()==0){
						_logger.warn("No host is available so the thread {} will sleep until one of the hosts becomes available.", Thread.currentThread().getName()); 
						lock.wait();
						_logger.info("The thread {} will resume as one of the hosts became available", Thread.currentThread().getName());
					}
					return getHost();
				} catch (InterruptedException e1) {
					_logger.error(e1.getMessage());
				}

			}
		}
		return result;
	}

	public void deleteHost(String nodeName) throws Exception{ 
		scheduler.deleteNode(nodeName);
	}

	public List<Host> getAllHosts(){
		return scheduler.getAllHosts();
	}

	public void dispose() {
		if(scheduler!=null){
			scheduler.dispose();
		}
	}

	/* Execute a request given by type requestType. */
	public HttpResponse executeHttpRequest(HttpMethod requestType, String path, StringEntity entity) throws Exception {
		HttpResponse httpResponse = null;
		StringBuilder url = new StringBuilder();
		url.append(getHost().getURL());
		if(path != null && path.length()>0){
			url.append('/');
			url.append(path);
		}
		_logger.info("Invoking URL: {}", url.toString());
		if (entity != null) {
			entity.setContentType("application/json");
		}
		try {
			switch (requestType) {
			case POST:

				HttpPost post = new HttpPost(url.toString());

				post.setEntity(entity);
				httpResponse = httpClient.execute(post);
				break;
			case GET:

				HttpGet httpGet = new HttpGet(url.toString());

				httpResponse = httpClient.execute(httpGet);
				break;
			case DELETE:

				HttpDelete httpDelete = new HttpDelete(url.toString());

				httpResponse = httpClient.execute(httpDelete);
				break;
			case PUT:

				HttpPut httpput = new HttpPut(url.toString());

				httpput.setEntity(entity);
				httpResponse = httpClient.execute(httpput);
				break;
			default:
				throw new MethodNotSupportedException(requestType.toString());
			}
		} catch (MethodNotSupportedException ex) {
			throw new Exception(ex);
		}
		return httpResponse;
	}

	private CloseableHttpClient createHttpClient(int connCount, int connTimeout, int socketTimeout, List<Host> hosts)  {
		PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();
		connMgr.setMaxTotal(connCount);

		for(Host host : hosts) {
			try{
				URL url = new URL(host.getURL()); 
				int port = url.getPort();
				if(port == -1){
					_logger.error("Invalid port for end point: {} so skipping this end point..", host.getURL());
					continue;
				}
				HttpHost httpHost = new HttpHost(url.getHost(),	url.getPort());
				connMgr.setMaxPerRoute(new HttpRoute(httpHost), connCount / hosts.size());
			}catch(MalformedURLException e){
				_logger.warn("The host {} is invalid so ignoring..", host.getURL()); 
			}
		}

		RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(connTimeout).setConnectTimeout(connTimeout).setSocketTimeout(
				socketTimeout).build();

		return HttpClients.custom().setConnectionManager(connMgr).setDefaultRequestConfig(reqConfig).build();
	}

	public CloseableHttpClient getHttpClient(){
		return httpClient;
	}

	private CloseableHttpClient recreateHttpClient(int connCount, int connTimeout, int socketTimeout, List<Host> hosts) {
		if(httpClient != null){
			try {
				httpClient.close();
			} catch (IOException e) {
				_logger.warn(e.getMessage()); 
			}
		}
		return createHttpClient(connCount, connTimeout, socketTimeout, hosts);
	}

	public void stateChanged(ChangeEvent e) {
		synchronized (lock) {
			updateInProgress=true;
			scheduler.updateHosts();
			httpClient = recreateHttpClient(connectionCount, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, getAllHosts());
			updateInProgress=false;
			lock.notifyAll();
		}


	}

	public static enum HttpMethod {
		/** GET operation. */
		GET,
		/** POST operation. */
		POST,
		/** PUT operation. */
		PUT,
		/** DELETE operation. */
		DELETE;

	}
}
/* Copyright (c) 2016, Salesforce.com, Inc.  All rights reserved. */