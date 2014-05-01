package at.ac.uibk.dps.biooozie.action;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

import at.ac.uibk.dps.biohadoop.hadoop.Client;
import at.ac.uibk.dps.biohadoop.torename.Hostname;
import at.ac.uibk.dps.biohadoop.torename.LocalResourceBuilder;

public class BiohadoopAction extends ActionExecutor {

	private static final String NODENAME = "biohadoop";
	private static final String SUCCEEDED = "SUCCEEDED";
	private static final String FAILED = "FAIL";
	private static final String KILLED = "KILLED";

	private static final String CONFIG_FILE = "config-file";
	protected XLog log = XLog.getLog(getClass());

	public BiohadoopAction() {
		super(NODENAME);
		log.error("***** BiohadoopAction *****");
	}

	public void initActionType() {
		log.error("***** initActionType *****");
	}

	@Override
	public void start(Context context, WorkflowAction action)
			throws ActionExecutorException {
		log.error("***** start *****");
		// Get parameters from Node configuration
		try {
			Element actionXml = XmlUtils.parseXml(action.getConf());
			Namespace ns = Namespace
					.getNamespace("uri:custom:biohadoop-action:0.1");
			String configFile = actionXml.getChildTextTrim(CONFIG_FILE, ns);

			// Check if all needed parameters are there
			if (configFile == null) {
				throw new ActionExecutorException(ErrorType.FAILED,
						ErrorCode.E0000.toString(),
						"No configuration file specified, please specify one inside the tag <config>");
			}
			
			// Execute action synchronously
			YarnConfiguration conf = new YarnConfiguration();
			conf.set("fs.default.name", "hdfs://master:54310");
			conf.set("fs.defaultFS", "hdfs://master:54310");
			Client client = new Client();
			client.run(conf, new String[]{configFile});
					
//			run(conf, new String[] {"/tmp/biohadoop-0.0.1-SNAPSHOT.jar", "at.ac.uibk.dps.biohadoop.hadoop.Client",
//					"at.ac.uibk.dps.biohadoop.ga.worker.SocketGaWorker", "2" });

			context.setExecutionData(SUCCEEDED, null);
		} catch (Exception e) {
			context.setExecutionData(FAILED, null);
			log.error("Error while running Oozie job", e);
			throw new ActionExecutorException(ErrorType.FAILED,
					ErrorCode.E0000.toString(), e.getMessage());
		} catch (Throwable e) {
			context.setExecutionData(FAILED, null);
			log.error("Error while running Oozie job", e);
			throw new ActionExecutorException(ErrorType.FAILED,
					ErrorCode.E0000.toString(), e.getMessage());
		}
	}

	@Override
	public void end(Context context, WorkflowAction action)
			throws ActionExecutorException {
		String externalStatus = action.getExternalStatus();
		WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ? WorkflowAction.Status.OK
				: WorkflowAction.Status.ERROR;
		context.setEndData(status, getActionSignal(status));
	}

	@Override
	public void check(Context context, WorkflowAction action)
			throws ActionExecutorException {
	}

	@Override
	public void kill(Context context, WorkflowAction action)
			throws ActionExecutorException {
		context.setExternalStatus(KILLED);
		context.setExecutionData(KILLED, null);
	}

	@Override
	public boolean isCompleted(String externalStatus) {
		return false;
	}

	public void run(YarnConfiguration conf, String[] args) throws Exception {
		log.info("############ Starting client ############");

		final String algorithm = args[0];
		final String containerCount = args[1];

		// Create yarnClient
		log.error("***** fs.default.name " + conf.getRaw("fs.default.name"));
		log.error("***** fs.defaultFS " + conf.getRaw("fs.defaultFS"));
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		// Create application via yarnClient
		YarnClientApplication app = yarnClient.createApplication();

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records
				.newRecord(ContainerLaunchContext.class);
		amContainer.setCommands(Collections.singletonList("$JAVA_HOME/bin/java"
				+ " -Xmx256M"
				+ " at.ac.uibk.dps.biohadoop.hadoop.ApplicationMaster "
				+ algorithm + " " + containerCount + " 1>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
				+ " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
				+ "/stderr"));

		// Set libs
		String libPath = "hdfs://" + Hostname.getHostname() + ":54310/biohadoop/lib/";
		String dataPath = "hdfs://" + Hostname.getHostname() + ":54310/biohadoop/data/";
//		String libPath = "file:///tmp/lib/";
//		String dataPath = "file:///tmp/data/";
		Map<String, LocalResource> jars = LocalResourceBuilder
				.getStandardResources(libPath, conf);
		Map<String, LocalResource> data = LocalResourceBuilder
				.getStandardResources(dataPath, conf);
		Map<String, LocalResource> combinedFiles = new HashMap<String, LocalResource>();
		combinedFiles.putAll(jars);
		combinedFiles.putAll(data);
		amContainer.setLocalResources(combinedFiles);

		// Setup CLASSPATH for ApplicationMaster
		Map<String, String> appMasterEnv = new HashMap<String, String>();
		setupAppMasterEnv(appMasterEnv, conf);
		amContainer.setEnvironment(appMasterEnv);

		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(256);
		capability.setVirtualCores(1);

		// Finally, set-up ApplicationSubmissionContext for the application
		ApplicationSubmissionContext appContext = app
				.getApplicationSubmissionContext();
		appContext.setApplicationName("biohadoop"); // application name
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		appContext.setQueue("default"); // queue

		// Submit application
		ApplicationId appId = appContext.getApplicationId();
		log.info("Submitting application " + appId);
		yarnClient.submitApplication(appContext);

		ApplicationReport appReport = yarnClient.getApplicationReport(appId);
		YarnApplicationState appState = appReport.getYarnApplicationState();

		log.info("Tracking URL: " + appReport.getTrackingUrl());
		log.info("Application Master running at: " + appReport.getHost());

		while (appState != YarnApplicationState.FINISHED
				&& appState != YarnApplicationState.KILLED
				&& appState != YarnApplicationState.FAILED) {
			Thread.sleep(100);
			appReport = yarnClient.getApplicationReport(appId);
			appState = appReport.getYarnApplicationState();
			log.info("Progress: " + appReport.getProgress());
		}

		log.info("Application " + appId + " finished with" + " state "
				+ appState + " at " + appReport.getFinishTime());
	}

	private void setupAppMasterEnv(Map<String, String> appMasterEnv,
			Configuration conf) {
		Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
				Environment.PWD.$() + File.separator + "*");
		for (String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
					c.trim());
		}
	}

}
