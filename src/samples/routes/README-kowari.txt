1) Create a model.
   trippi -c
   create;
   For class name, enter org.trippi.impl.kowari.KowariConnector
   For profile id, enter ktest.
   For model name enter ktest.
   For other prompts, make it a local, writable, autoCreate, memoryBuffer model.

2) Load the rdf.
   trippi -c ktest
   load path/to/routes.xml;
   load path/to/airports.xml;
   load path/to/countries.xml;
   load path/to/states.xml;

3) Enter the text for the queries as shown below.
   
#
# Tell me the number of miles, 
#         the origin airport name,
#     and the destination airport name
# Of every route in <#ktest> that is less than 36 miles.
#
tuples
itql
select    $miles 
          $fromName
          $toName
from      <#ktest>
where         $route <travel:miles>     $miles
          and $miles <tucana:lt>        '36'      in <#xsd>
          and $route <cyc:fromLocation> $from
          and $route <cyc:toLocation>   $to
          and $from  <dc:title>         $fromName
          and $to    <dc:title>         $toName
order by  $miles;


#
# . . . as a sub-graph of the original
#
triples ( $route     <travel:miles>     $miles 
          $route     <cyc:fromLocation> $from
          $route     <cyc:toLocation>   $to
          $from      <dc:title>         $fromName
          $to        <dc:title>         $toName )
itql
select    $route
          $miles
          $from 
          $fromName
          $to
          $toName
from      <#ktest>
where         $route <travel:miles>     $miles
          and $miles <tucana:lt>        '36'      in <#xsd>
          and $route <cyc:fromLocation> $from
          and $route <cyc:toLocation>   $to
          and $from  <dc:title>         $fromName
          and $to    <dc:title>         $toName
order by  $miles;

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
itql
select    $miles 
          $fromName
          $toName
from      <#ktest>
where         $route <travel:miles>     $miles
          and $miles <tucana:lt>        '36'      in <#xsd>
          and $route <cyc:fromLocation> $from
          and $route <cyc:toLocation>   $to
          and $from  <dc:title>         $fromName
          and $to    <dc:title>         $toName
order by  $miles;