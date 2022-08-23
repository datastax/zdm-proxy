# DataStax ZDM-Proxy documentation

Since September 2021, the ZDM team has provided detailed internal documentation for the DataStax Services team. So far, migrations have been 
Services-led engagements using the Cloudgate Proxy (now, the “DataStax ZDM Proxy”), related tools, and advice from the extended ZDM team. 

POCs, refinements to the instructions, and evolving sets of tools have resulted in several successful Cassandra or DSE migrations to cloud-native Astra 
DB environments, with double writes to origin and target clusters. 

Now it’s time to assemble the details into external documentation for the ZDM-Proxy 2.0 release. 

This docs-content directory will contain the Asciidoc source files and Antora playbooks to create the ZDM-Proxy user documentation.

Built output is temporarily [here](https://coppi.sjc.dsinternal.org/en/zdm/docs/index.html) on a DataStax internal review server. You'll need 
to set up the GlobalProtect app to access it via VPN.
