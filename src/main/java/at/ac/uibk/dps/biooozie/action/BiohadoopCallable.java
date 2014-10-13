package at.ac.uibk.dps.biooozie.action;

import java.util.concurrent.Callable;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import at.ac.uibk.dps.biohadoop.hadoop.BiohadoopClient;

public class BiohadoopCallable implements Callable<FinalApplicationStatus> {

	private final YarnConfiguration yarnConfiguration;
	private final String configFile;

	public BiohadoopCallable(YarnConfiguration yarnConfiguration,
			String configFile) {
		this.yarnConfiguration = yarnConfiguration;
		this.configFile = configFile;
	}

	@Override
	public FinalApplicationStatus call() throws Exception {
		BiohadoopClient client = new BiohadoopClient();
		return client.run(yarnConfiguration, new String[] { configFile });
	}

}
