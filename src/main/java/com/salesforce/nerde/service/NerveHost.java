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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Encapsulates host information
 * @author  Raj Sarkapally (rsarkapally@salesforce.com)
 *
 */
@JsonIgnoreProperties(value={ "url" }, allowGetters=true)
public class NerveHost implements Host{
	private static String DEFAULT_PROTOCOL="http://";
	private String host;
	private int port;
	private String name;
	private String protocol;
	@JsonIgnore
	private String url; //To improve the performance

	public NerveHost(){};
	public NerveHost(String host, int port, String name){
		this(host, port, name, DEFAULT_PROTOCOL);
	}
	public NerveHost(String host, int port, String name, String protocol){
		this.host=host;
		this.port=port;
		this.name=name;
		this.protocol=protocol;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getProtocol() {
		return protocol;
	}
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	@JsonIgnore
	public String getURL(){
		if(url==null){
			StringBuilder result=new StringBuilder((protocol !=null && protocol.length()>0)?protocol:DEFAULT_PROTOCOL);
			result.append(host);
			if(port>0){
				result.append(':').append(port);
			}
			url= result.toString();
		}
		return url;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}

		final NerveHost other = (NerveHost) obj;

		if (!Objects.equals(this.host, other.host)) {
			return false;
		}
		if (!Objects.equals(this.port, other.port)) {
			return false;
		}
		if (!Objects.equals(this.protocol, other.protocol)) {
			return false;
		}
		return true;
	}
	@Override
	public int hashCode() {
		int hash = 5;
		hash = 31 * hash + Objects.hashCode(this.protocol);
		hash = 31 * hash + Objects.hashCode(this.host);
		hash = 31 * hash + Objects.hashCode(this.port);
		return hash;
	}

}
