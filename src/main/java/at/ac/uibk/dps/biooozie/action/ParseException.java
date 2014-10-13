package at.ac.uibk.dps.biooozie.action;

public class ParseException extends Exception {

	private static final long serialVersionUID = -5565485524664271277L;

	public ParseException() {
	}

	public ParseException(String message) {
		super(message);
	}

	public ParseException(Throwable cause) {
		super(cause);
	}

	public ParseException(String message, Throwable cause) {
		super(message, cause);
	}
}
