package com.salesforce.nerde.exception;

@SuppressWarnings("serial")
public class NoHostException extends RuntimeException{
	public NoHostException(String message){
		super(message);
	}
}
