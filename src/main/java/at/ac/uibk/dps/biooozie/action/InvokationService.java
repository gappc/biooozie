package at.ac.uibk.dps.biooozie.action;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XLog;

public class InvokationService {

	private static final XLog LOG = XLog.getLog(InvokationService.class);
	
	public static InvokationController invokeAllBiohadoops(final WorkflowAction action)
			throws ActionExecutorException {
		final Configuration configuration = getConfiguration(action);
		return invokeAll(configuration);
	}

	private static Configuration getConfiguration(final WorkflowAction action)
			throws ActionExecutorException {
		try {
			final ConfigurationParser parser = new ConfigurationParser();
			return parser.getBioozieConfiguration(action);
		} catch (ParseException e) {
			throw new ActionExecutorException(ErrorType.FAILED, Constants.B001,
					"Could not parse workflow.xml", e);
		}
	}

	private static InvokationController invokeAll(Configuration configuration) {
		final List<Callable<FinalApplicationStatus>> callables = prepareBiohadoopCallables(configuration);
		final List<Future<FinalApplicationStatus>> invocations = new ArrayList<>();

		final ExecutorService executorService = Executors
				.newFixedThreadPool(callables.size());
		final ExecutorCompletionService<FinalApplicationStatus> completionService = new ExecutorCompletionService<>(
				executorService);

		for (Callable<FinalApplicationStatus> biohadoopCallable : callables) {
			LOG.info("Invoking Biohadoop");
			Future<FinalApplicationStatus> invocation = completionService
					.submit(biohadoopCallable);
			invocations.add(invocation);
		}
		executorService.shutdown();

		return new InvokationController(completionService, invocations);
	}

	private static List<Callable<FinalApplicationStatus>> prepareBiohadoopCallables(
			Configuration configuration) {
		final List<Callable<FinalApplicationStatus>> biohadoopCallables = new ArrayList<>();
		final YarnConfiguration yarnConfiguration = getYarnConfiguration(configuration
				.getNameNode());
		for (String configFile : configuration.getConfigFiles()) {
			biohadoopCallables.add(new BiohadoopCallable(yarnConfiguration,
					configFile));
		}
		return biohadoopCallables;
	}

	private static YarnConfiguration getYarnConfiguration(String nameNode) {
		final YarnConfiguration yarnConfiguration = new YarnConfiguration();
		yarnConfiguration.set("fs.default.name", nameNode);
		yarnConfiguration.set("fs.defaultFS", nameNode);
		return yarnConfiguration;
	}
	
}
