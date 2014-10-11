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
import org.apache.oozie.command.wf.ActionEndXCommand;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

import at.ac.uibk.dps.biohadoop.hadoop.BiohadoopClient;

public class ParallelBiohadoopAction extends ActionExecutor {

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

	public ParallelBiohadoopAction() {
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

			Callable<String> bioadoop = new Callable<String>() {

				@Override
				public String call() throws Exception {
					log.error("##### BEFORE RUNNING BiohadoopClient #####");

					check(context, action);

					BiohadoopClient client = new BiohadoopClient();
					try {
						client.run(conf, new String[] { configFile });
					} catch (Exception e) {
						log.error("OOZIE ERROR", e);
					}

					log.error("##### AFTER RUNNING BiohadoopClient #####");

					finished.set(true);
					check(context, action);

					return "AHA";
				}
			};

			ExecutorService executor = Executors.newCachedThreadPool();
			biohadoopResult = executor.submit(bioadoop);
			executor.shutdown();

			// run(conf, new String[]
			// {"/tmp/biohadoop-0.0.1-SNAPSHOT.jar",
			// "at.ac.uibk.dps.biohadoop.hadoop.Client",
			// "at.ac.uibk.dps.biohadoop.ga.worker.SocketGaWorker", "2"
			// });

			// context.setExecutionData(SUCCEEDED, null);
			// }
			// }).start();

			// String callbackPost = ignoreOutput ? "_" : getOozieConf().get(
			// HTTP_COMMAND_OPTIONS).replace(" ", "%%%");

			String callBackUrl = Services.get().get(CallbackService.class)
					.createCallBackUrl(action.getId(), "finished");

			log.error("##### CALLBACKURL: " + callBackUrl);

			log.info("##### Setting start data #####");

			log.info("##### externalId: " + action.getExternalId());
			log.info("##### trackerUri: " + action.getTrackerUri());
			log.info("##### consoleUrl: " + action.getConsoleUrl());
			String externalId = action.getId();
			String trackerUri = "http://localhost/trackerUri";
			String consoleUrl = "http://localhost/consoleUrl";
			context.setStartData(externalId, callBackUrl, callBackUrl);

			// If this is enabled, the workflow only works in sequential mode
			// (fork/join is computed sequentially), but the result is set to ok
//			biohadoopResult.get();
//			log.info("shutting down");
//			context.setExecutionData("SUCCEEDED", null);
//			context.setEndData(Status.OK, Status.OK.toString());
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
	public void end(Context context, WorkflowAction action)
			throws ActionExecutorException {
		log.error("##### GOT end...");

		// context.setExecutionData(Status.OK.toString(), null);
		// context.setExternalStatus(Status.OK.toString());

		context.setEndData(Status.OK, Status.OK.toString());

		// XCallable<Void> command = null;
		// command = new ActionEndXCommand(action.getId(),
		// Status.DONE.toString());
		// if (!Services.get().get(CallableQueueService.class).queue(command)) {
		// log.warn(XLog.OPS,
		// "queue is full or system is in SAFEMODE, ignoring callback");
		// }
		// log.error("done is queued");
		//
		// command = new ActionEndXCommand(action.getId(),
		// Status.OK.toString());
		// if (!Services.get().get(CallableQueueService.class).queue(command)) {
		// log.warn(XLog.OPS,
		// "queue is full or system is in SAFEMODE, ignoring callback");
		// }
		// log.error("ok is queued");

		// log.error("##### DOING SHUTDOWN FOR ACTION " + action.getId());
		// XCallable<Void> command = null;
		// command = new CompletedActionXCommand(action.getId(),
		// Status.OK.toString(),
		// null, 2);
		// if (!Services.get().get(CallableQueueService.class).queue(command)) {
		// log.warn(XLog.OPS,
		// "queue is full or system is in SAFEMODE, ignoring callback");
		// }
		// log.error("shutdown queued");

		// CallbackService callbackService =
		// Services.get().get(CallbackService.class);
		// DagEngine dagEngine =
		// Services.get().get(DagEngineService.class).getSystemDagEngine();
		// try {
		// dagEngine.processCallback(action.getId(), Status.OK.toString(),
		// null);
		// } catch (DagEngineException e) {
		// log.error("######## error with dagEngine", e);
		// }

		// if (action.getExternalStatus().equals("OK")) {
		// context.setEndData(WorkflowAction.Status.OK,
		// WorkflowAction.Status.OK.toString());
		// } else {
		// context.setEndData(WorkflowAction.Status.ERROR,
		// WorkflowAction.Status.ERROR.toString());
		// }
		//
		// String externalStatus = action.getExternalStatus();
		// WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ?
		// WorkflowAction.Status.OK
		// : WorkflowAction.Status.ERROR;
		//
		// log.error("##### ...end with status " + status);
		//
		// switch (status) {
		// case ERROR:
		// context.setExecutionData(FAILED, null);
		// context.setExternalStatus(FAILED);
		// break;
		// case OK:
		// context.setExecutionData(SUCCEEDED, null);
		// context.setExternalStatus(SUCCEEDED);
		// break;
		// default:
		// context.setExecutionData(UNDEFINED, null);
		// context.setExternalStatus(UNDEFINED);
		// break;
		// }
		// context.setEndData(status, getActionSignal(status));
	}

	@Override
	public void check(final Context context, final WorkflowAction action)
			throws ActionExecutorException {
		log.error("##### GOT check (future is: " + biohadoopResult.isDone()
				+ ")");
		if (finished.get()) {
			log.error("##### STARTING SHUTDOWN THREAD");
			new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					log.error("##### GOT check with finished (future is: "
							+ biohadoopResult.isDone() + ")");

					context.setExternalStatus(Status.OK.toString());
					context.setExecutionData("SUCCEEDED", null);
					try {
						end(context, action);
					} catch (ActionExecutorException e) {
						log.error("####### GOT ActionExecutorException", e);
					}

					log.error("###### STATUS is: " + action.getStatus());
				}
			}).start();

			// context.setExternalStatus(SUCCEEDED);
			// context.setExecutionData(SUCCEEDED, null);
			//
			// String externalStatus = action.getExternalStatus();
			// WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ?
			// WorkflowAction.Status.OK
			// : WorkflowAction.Status.ERROR;
			//
			// context.setEndData(status, getActionSignal(status));
			// end(context, action);
		} else {
			log.error("##### GOT check with NOT finished");
			context.setExternalStatus(Status.RUNNING.toString());
		}
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
