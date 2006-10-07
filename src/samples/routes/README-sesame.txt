1) Create a model.
   trippi -c
   create;
   For class name, enter org.trippi.impl.sesame.SesameConnector
   For profile id, enter stest.
   For model name enter stest.
   Choose working values for other prompts.

2) Load the rdf.
   trippi -c stest
   load path/to/routes.xml;
   load path/to/airports.xml;
   load path/to/countries.xml;
   load path/to/states.xml;

3) Enter the text for the queries as shown below.
   
#
# Tell me the number of miles, 
#         the origin airport name,
#     and the destination airport name
# Of every route that is less than 36 miles.
#
tuples serql select miles, fromName, toName
from   {route}    travel:miles     {miles},
       {route}    cyc:fromLocation {fromLoc},
       {route}    cyc:toLocation   {toLoc},
       {fromLoc}  dc:title         {fromName},
       {toLoc}    dc:title         {toName}
where  miles < "36"^^xsd:double;

#
# . . . as a sub-graph of the original
#
triples serql construct *
from   {route}    travel:miles     {miles},
       {route}    cyc:fromLocation {fromLoc},
       {route}    cyc:toLocation   {toLoc},
       {fromLoc}  dc:title         {fromName},
       {toLoc}    dc:title         {toName}
where  miles < "36"^^xsd:double;

#
# . . . as a graph of query solutions
#
triples ( _solution  <result:binding>   _miles
          _miles     <result:name>      "miles"
          _miles     <result:value>     $miles
          _solution  <result:binding>   _fromName
          _fromName  <result:name>      "fromName"
          _fromName  <result:value>     $fromName
          _solution  <result:binding>   _toName
          _toName    <result:name>      "toName"
          _toName    <result:value>     $toName )
serql select miles, fromName, toName
from   {route}    travel:miles     {miles},
       {route}    cyc:fromLocation {fromLoc},
       {route}    cyc:toLocation   {toLoc},
       {fromLoc}  dc:title         {fromName},
       {toLoc}    dc:title         {toName}
where  miles < "36"^^xsd:double;

