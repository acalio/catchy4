package org.acalio.dm.api;

public class DeveloperKeyNotSetException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    public static String ERROR_MESSAGE = "You must set a  developer Key";

    public DeveloperKeyNotSetException() {
	super(ERROR_MESSAGE); 
    } 
    

}
