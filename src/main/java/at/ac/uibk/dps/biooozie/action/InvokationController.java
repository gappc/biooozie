package at.ac.uibk.dps.biooozie.action;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.util.XLog;

public class InvokationController {

	private final XLog log = XLog.getLog(getClass());

	private final ExecutorCompletionService<FinalApplicationStatus> completionService;
	private final List<Future<FinalApplicationStatus>> invokations;

	public InvokationController(final ExecutorCompletionService<FinalApplicationStatus> completionService,
			final List<Future<FinalApplicationStatus>> invokations) {
		this.completionService = completionService;
		this.invokations = invokations;
	}

	public void waitForCompletion() throws ActionExecutorException {
		try {
			for (int i = 0; i < invokations.size(); i++) {
				log.info("Waiting for Biohadoop Action " + i + " to complete");
				FinalApplicationStatus appState = completionService.take().get();
				log.info("Biohadoop Action " + i + " completed with state " + appState);
				checkAndThrow(appState);
			}
		} catch (InterruptedException e) {
			shutdownAllInvokations();
			throw new ActionExecutorException(ErrorType.FAILED, Constants.B002,
					"Interrupted while running Biohadoop", e);
		} catch (ExecutionException e) {
			shutdownAllInvokations();
			throw new ActionExecutorException(ErrorType.FAILED, Constants.B003,
					"Exception in Biohadoop", e);
		}
	}

	private void checkAndThrow(FinalApplicationStatus appState) throws ActionExecutorException {
		if (appState == FinalApplicationStatus.FAILED) {
			throw new ActionExecutorException(ErrorType.FAILED, Constants.B004,
					"Biohadoop instance failed");
		}
		if (appState == FinalApplicationStatus.KILLED) {
			throw new ActionExecutorException(ErrorType.FAILED, Constants.B005,
					"Biohadoop instance killed");
		}
		if (appState == FinalApplicationStatus.UNDEFINED) {
			throw new ActionExecutorException(ErrorType.FAILED, Constants.B006,
					"Biohadoop instance finished undefined");
		}
	}

	private void shutdownAllInvokations() {
		log.error("Got Exception while running Biohadoop, shutting all down");
		for (Future<FinalApplicationStatus> invokation : invokations) {
			invokation.cancel(true);
		}
	}
}
