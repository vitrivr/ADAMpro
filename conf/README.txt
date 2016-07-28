The conf folder is automatically included to the resources
(see line unmanagedResourceDirectories in Compile += baseDirectory.value / "conf" in build.sbt).

Make sure that the files here can be included. For instance, the core-site.xml files in this folder are adopted
by the system (i.e., by SQLContext).