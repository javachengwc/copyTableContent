package org.saiku.copyTableContent.exception;

public class CopyTableException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6092747691536913060L;
	
	private String errorMsg;
	
	public CopyTableException(String msg){
		this.errorMsg = msg;
	}
	public void setMessage(String errorMsg){
		this.errorMsg = errorMsg;
	}
	
	public String getMessage(){
		return this.errorMsg;
	}
}
