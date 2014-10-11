package at.ac.uibk.dps.biooozie.action;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

import at.ac.uibk.dps.biohadoop.hadoop.BiohadoopClient;

public class BiohadoopAction extends ActionExecutor {

	private static final String NODENAME = "biohadoop";
	// private static final String RUNNING = "RUNNING";
	// private static final String SUCCEEDED = "SUCCEEDED";
	// private static final String FAILED = "FAIL";
	// private static final String KILLED = "KILLED";
	// private static final String UNDEFINED = "UNDEFINED";

	private static final String CONFIG_FILE = "config-file";

	private final AtomicBoolean finished = new AtomicBoolean(false);
	private Future<String> biohadoopResult = null;

	protected XLog log = XLog.getLog(getClass());

	public BiohadoopAction() {
		super(NODENAME);
		log.error("##### BiohadoopAction #####");
	}

	public void initActionType() {
		super.initActionType();
		log.error("##### initActionType #####");
	}

	@Override
	public void start(final Context context, final WorkflowAction action)
			throws ActionExecutorException {
		log.error("##### start #####");

		// Get parameters from Node configuration
		try {
			Element actionXml = XmlUtils.parseXml(action.getConf());
			Namespace ns = Namespace
					.getNamespace("uri:custom:biohadoop-action:0.1");
			final String configFile = actionXml.getChildTextTrim(CONFIG_FILE,
					ns);

			// Check if all needed parameters are there
			if (configFile == null) {
				throw new ActionExecutorException(ErrorType.FAILED,
						ErrorCode.E0000.toString(),
						"No configuration file specified, please specify one inside the tag <config>");
			}

			final YarnConfiguration conf = new YarnConfiguration();
			conf.set("fs.default.name", "hdfs://master:54310");
			conf.set("fs.defaultFS", "hdfs://master:54310");

			Callable<String> biohadoop = new Callable<String>() {
		        @Override
		        public String call() throws Exception {
		            log.info("Starting Biohadoop");

		            // No difference if check() is called manually
		            // or if the next line is commented out
		            check(context, action);

		            BiohadoopClient client = new BiohadoopClient();
		            client.run(conf, new String[] { configFile });
		            log.info("Biohadoop finished");             

		            finished.set(true);
		            // No difference if check() is called manually
		            // or if the next line is commented out
		            check(context, action);
		            end(context, action);

		            return null;
		        }
		    };

		    ExecutorService executor = Executors.newCachedThreadPool();
		    biohadoopResult = executor.submit(biohadoop);
		    executor.shutdown();

			log.info("##### Setting start data #####");

			String externalId = action.getId();
		    String callBackUrl = context.getCallbackUrl("finished");
		    context.setStartData(externalId, callBackUrl, callBackUrl);
		} catch (Exception e) {
			context.setExecutionData(Status.ERROR.toString(), null);
			log.error("Error while running Oozie job", e);
			throw new ActionExecutorException(ErrorType.FAILED,
					ErrorCode.E0000.toString(), e.getMessage());
		} catch (Throwable e) {
			context.setExecutionData(Status.ERROR.toString(), null);
			log.error("Error while running Oozie job", e);
			throw new ActionExecutorException(ErrorType.FAILED,
					ErrorCode.E0000.toString(), e.getMessage());
		}
	}
	
	@Override
	public void check(final Context context, final WorkflowAction action)
			throws ActionExecutorException {
		// finished is an AtomicBoolean, that is set to true,
	    // after Biohadoop has finished (see implementation of Callable)
	    if (finished.get()) {
	        log.info("check(Context, WorkflowAction) invoked - Callable has finished");
	        context.setExternalStatus(Status.OK.toString());
	        context.setExecutionData(Status.OK.toString(), null);
	    } else {
	        log.info("check(Context, WorkflowAction) invoked");
	        context.setExternalStatus(Status.RUNNING.toString());
	    }
	}

	@Override
	public void end(Context context, WorkflowAction action)
			throws ActionExecutorException {
		log.info("end(Context, WorkflowAction) invoked");
	    context.setEndData(Status.OK, Status.OK.toString());
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
