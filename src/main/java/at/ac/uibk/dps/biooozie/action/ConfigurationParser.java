package at.ac.uibk.dps.biooozie.action;

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class ConfigurationParser {

	private final XLog log = XLog.getLog(getClass());

	public Configuration getBioozieConfiguration(final WorkflowAction action)
			throws ParseException {
		final Element rootElement = parseAction(action);
		final String nameNode = getNameNode(rootElement);
		final List<String> configFiles = getConfigFiles(rootElement);
		return new Configuration(nameNode, configFiles);
	}

	private Element parseAction(final WorkflowAction action)
			throws ParseException {
		try {
			return XmlUtils.parseXml(action.getConf());
		} catch (JDOMException e) {
			throw new ParseException("Could not parse workflow.xml");
		}
	}

	private String getNameNode(final Element rootElement) {
		final Element nameNodeElement = rootElement.getChild(
				Constants.NODE_NAME_NODE, Constants.NAMESPACE);
		return nameNodeElement.getTextTrim();
	}

	private List<String> getConfigFiles(final Element rootElement) {
		@SuppressWarnings("unchecked")
		final List<Element> configElements = rootElement.getChildren(
				Constants.NODE_CONFIG_FILE, Constants.NAMESPACE);
		final List<String> configFiles = new ArrayList<>();
		for (Element configElement : configElements) {
			final String configFile = configElement.getTextTrim();
			configFiles.add(configFile);
			log.info("<{0}>" + configFile + "</{0}>",
					Constants.NODE_CONFIG_FILE);
		}
		return configFiles;
	}

}
