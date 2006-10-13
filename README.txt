This is Trippi v1.1.1 from http://www.sourceforge.net/projects/trippi

CONTRIBUTORS
------------
The following people contributed to this release:

  - Stephen Bayliss of Rightscom (Remote Kowari support)
  - Sam Liberman of Case Western (Oracle Spatial Connector)


COMPATIBILITY
-------------
This version works with the following Triplestores:

  - Kowari 1.0.5 (patched) from http://sourceforge.net/projects/kowari
  - Sesame 1.2RC2          from http://openrdf.org/
  - Oracle 10g Spatial     from http://oracle.com/
  - MPTStore 0.9.1         from http://mptstore.sourceforge.net/

This version includes, and depends on JRDF v0.3.3.


KOWARI PATCH INFORMATION
------------------------
The driver-1.0.5.jar that comes with this distribution has
several patches applied from the released version.  
Further information about the patches (including the source code)
can be found in lib/kowari-1.0.5-patches.zip


IMPORTANT INFORMATION RE:COMPILING WITH ORACLE
----------------------------------------------
Due to license restrictions, we cannot include the appropriate
Oracle JDBC driver in the source or binary distribution.

However, currently it is required in order to compile.
This will be addressed.

In the mean time, you can download the ojdbc14.jar from
the following location:

  http://www.oracle.com/technology/software/tech/java/sqlj_jdbc/index.html

Put it in lib/, then build.


IMPORTANT INFORMATION RE:JAVA VERSION SUPPORT
---------------------------------------------
This release compiles and runs with Java 1.4 and 1.5.
Future releases are likely to require Java 1.5.

