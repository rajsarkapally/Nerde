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
package com.salesforce.nerde;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.nerde.service.ResourceDiscoveryService;
import com.salesforce.nerde.service.Host;
import com.salesforce.nerde.service.NerveHost;

import junit.framework.TestCase;
public class ResourceDiscoveryServiceTest extends TestCase {
	private TestingServer zkTestServer;
	private String servicePath="/nerve/services";
	CuratorFramework client;
	@Before
	public void setUp() {
		try {
			zkTestServer = new TestingServer(2182);
			client=CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(),new RetryOneTime(1000));
		} catch (Exception ex) {
			fail("Exception during zookeeper startup." + "Reason:" + ex.toString());
		}
	}

	@After
	public void tearDown() {
		try {
			client.close();
			zkTestServer.close();
			Field discoveryServiceMapField = ResourceDiscoveryService.class.getDeclaredField("discoveryServiceMap");
			discoveryServiceMapField.setAccessible(true);
			discoveryServiceMapField.set(ResourceDiscoveryService.class, new HashMap<String, ResourceDiscoveryService>()); 
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Test
	public void testWhenTwoActiveNodesReturnsTwoNodes(){
		ObjectMapper mapper=new ObjectMapper();
		NerveHost host1=new NerveHost("www.host1.com",1111,"host1",null);
		NerveHost host2=new NerveHost("www.host2.com",2222,"host2",null);
		client.start();
		try{
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+host1.getName(),mapper.writeValueAsBytes(host1));
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+ host2.getName(),mapper.writeValueAsBytes(host2));
		}catch(Exception e){
			fail(e.getMessage());
		}
		ResourceDiscoveryService service=ResourceDiscoveryService.getInstance(zkTestServer.getConnectString(), servicePath,2);
		List<Host> actual = service.getAllHosts();
		List<NerveHost> expected = Arrays.asList(host1,host2);
		assertEquals(2, actual.size());
		assertEquals(expected, actual);
	}

	@Test
	public void testWhenTwoActiveNodesReturnsNodesInRoundRobin(){
		ResourceDiscoveryService service=ResourceDiscoveryService.getInstance(zkTestServer.getConnectString(), servicePath,2);
		ObjectMapper mapper=new ObjectMapper();
		NerveHost host1=new NerveHost("www.host1.com",1111,"host1",null);
		NerveHost host2=new NerveHost("www.host2.com",2222,"host2",null);
		client.start();
		try{
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+ host1.getName(),mapper.writeValueAsBytes(host1));
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+ host2.getName(),mapper.writeValueAsBytes(host2));
			Thread.sleep(1000);
		}catch(Exception e){
			fail(e.getMessage());
		}
		Set<Host> expected = new HashSet<Host>();
		expected.add(host1);
		expected.add(host2);
		Set<Host> actual = new HashSet<Host>();
		try{
			actual.add(service.getHost());
			actual.add(service.getHost());
		}catch(Exception e){
			fail(e.getMessage());
		}
		assertEquals(expected, actual);
	}

	@Test
	public void testWhenTwoActiveNodesThenDeleteOneNodeReturnsSingleNode(){
		ObjectMapper mapper=new ObjectMapper();
		ResourceDiscoveryService service=ResourceDiscoveryService.getInstance(zkTestServer.getConnectString(), servicePath,2);
		NerveHost host1=new NerveHost("www.host3.com",1111,"host3",null);
		NerveHost host2=new NerveHost("www.host4.com",2222,"host4",null);

		client.start();
		try{
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+ host1.getName(),mapper.writeValueAsBytes(host1));
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+ host2.getName(),mapper.writeValueAsBytes(host2));
			Thread.sleep(1000);
			client.delete().forPath(servicePath+"/"+host1.getName());
			Thread.sleep(1000);
		}catch(Exception e){
			fail(e.getMessage());
		}
		Host expected=host2;
		assertEquals(1, service.getAllHosts().size());
		Host actual=null;
		try{
			actual=service.getHost();
		}catch(Exception e){
			fail(e.getMessage());
		}
		assertEquals(expected,actual);
	}

	@Test
	public void testWhenOneActiveNodeThenDeleteNodeReturnsZeroNode() {
		ObjectMapper mapper=new ObjectMapper();
		ResourceDiscoveryService service=ResourceDiscoveryService.getInstance(zkTestServer.getConnectString(), servicePath,2);
		NerveHost host1=new NerveHost("www.host1.com",1111,"host1",null);
		client.start();
		try{
			client.create().creatingParentContainersIfNeeded().forPath(servicePath+"/"+ host1.getName(),mapper.writeValueAsBytes(host1));
			Thread.sleep(1000);
			service.deleteHost(host1.getName());
			Thread.sleep(1000);
		}catch(Exception e){
			fail(e.getMessage());
		}
		assertEquals(0, service.getAllHosts().size());     
	}
}
/* Copyright (c) 2016, Salesforce.com, Inc.  All rights reserved. */
