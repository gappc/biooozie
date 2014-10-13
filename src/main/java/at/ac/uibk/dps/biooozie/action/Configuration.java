package at.ac.uibk.dps.biooozie.action;

import java.util.ArrayList;
import java.util.List;

public class Configuration {

	private final List<String> configFiles;
	private final String nameNode;

	public Configuration(String nameNode, List<String> configFiles) {
		this.nameNode = nameNode;
		if (configFiles != null) {
			this.configFiles = configFiles;
		} else {
			this.configFiles = new ArrayList<>();
		}
	}

	public List<String> getConfigFiles() {
		return configFiles;
	}

	public String getNameNode() {
		return nameNode;
	}

}
