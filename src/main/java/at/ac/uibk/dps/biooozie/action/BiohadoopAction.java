package at.ac.uibk.dps.biooozie.action;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.util.XLog;

public class BiohadoopAction extends ActionExecutor {

	private final XLog log = XLog.getLog(getClass());
	private final String SUCCEEDED = "SUCCEEDED";

	public BiohadoopAction() {
		super(Constants.NODE_ROOT_ELEMENT);
		log.debug("New BiohadoopAction created");
	}

	public void initActionType() {
		super.initActionType();
		log.debug("Invoked initActionType()");
	}

	@Override
	public void start(final Context context, final WorkflowAction action)
			throws ActionExecutorException {
		log.debug("Invoked start(Context, WorkflowAction)");

		final String callbackUrl = context.getCallbackUrl("status");
		context.setStartData(action.getId(), callbackUrl, callbackUrl);

		final InvokationController invoker = InvokationService.invokeAllBiohadoops(action);
		invoker.waitForCompletion();
		log.info("All Biohadoop actions completed successfully");

		context.setExecutionData(SUCCEEDED, null);
	}

	@Override
	public void check(final Context context, final WorkflowAction action)
			throws ActionExecutorException {
	}

	@Override
	public void end(Context context, WorkflowAction action)
			throws ActionExecutorException {
		log.info("end(Context, WorkflowAction) invoked");
		String externalStatus = action.getExternalStatus();
		WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ? WorkflowAction.Status.OK
				: WorkflowAction.Status.ERROR;
		context.setEndData(status, getActionSignal(status));
	}

	@Override
	public void kill(Context context, WorkflowAction action)
			throws ActionExecutorException {
		context.setExternalStatus(Status.KILLED.toString());
		context.setExecutionData(Status.KILLED.toString(), null);
	}

	@Override
	public boolean isCompleted(String externalStatus) {
		return false;
	}

}
