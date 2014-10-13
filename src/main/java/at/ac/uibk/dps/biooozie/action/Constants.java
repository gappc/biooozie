package at.ac.uibk.dps.biooozie.action;

import org.jdom.Namespace;

public class Constants {
	public static final Namespace NAMESPACE = Namespace
			.getNamespace("uri:custom:biohadoop-action:0.1");
	public static final String NODE_ROOT_ELEMENT = "biohadoop";
	public static final String NODE_NAME_NODE = "name-node";
	public static final String NODE_CONFIG_FILE = "config-file";

	// Error when parsing workflow.xml
	public static final String B001 = "B001";
	// Interrupted while running Biohadoop
	public static final String B002 = "B002";
	// Exception in Biohadoop
	public static final String B003 = "B003";
	// Biohadoop instance failed
	public static final String B004 = "B004";
	// Biohadoop instance killed
	public static final String B005 = "B005";
	// Biohadoop instance finished undefined
	public static final String B006 = "B006";
}
