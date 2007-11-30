README.txt

The following are the patches that have been applied against Mulgara 1.1.1

1) ModelExistsOperation.patch
	- addresses a bug that manifests with a local (in-JVM) instance of Mulgara.
	
2) RemoteSessionFactoryImpl.patch
	- addresses a bug that closed the underlying SessionFactory.
	
2) SPDateTimeImpl.patch, XSD.patch, SPDateTimeUnitTest.patch
	- returns XSD dateTimes as UTC timezoned values.

3) build.xml.patch, build.properties.patch
	- adds a new build target "core-dist" which produces mulgara-core-1.1.1.jar.