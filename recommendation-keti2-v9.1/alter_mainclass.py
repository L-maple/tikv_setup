#!/usr/bin/python
# -*- coding: UTF-8 -*-
 
from xml.etree.ElementTree import parse, register_namespace
import sys

def alterMainClass(mainClassName):
   filepath="pom.xml"

   # register the namespace 
   register_namespace('', "http://maven.apache.org/POM/4.0.0")
   register_namespace('xsi', "http://www.w3.org/2001/XMLSchema-instance")

   # get the root of the xml tree
   tree = parse(filepath)
   root = tree.getroot()
   
   # set the tag prefix
   prefix  = "{http://maven.apache.org/POM/4.0.0}"
   build   = root.find(prefix + "build")
   plugins = build.find(prefix + "plugins")

   for plugin in plugins:
      executions = plugin.find(prefix + "executions")
      if executions is not None:
         execution = executions.find(prefix + "execution")
         if execution is not None:
            configuration = execution.find(prefix + "configuration")
            if configuration is not None:
               transformers  = configuration.find(prefix + "transformers")
               for transformer in transformers:
                  mainClass = transformer.find(prefix + 'mainClass')
                  if mainClass is not None:
                     mainClass.text = mainClassName

   # write the changes
   tree.write(filepath)
   print "mainclass has been changed to " + mainClassName + "."


if __name__ == "__main__":
   if len(sys.argv) <= 1:
      exit("usage: python docker_generator.py <mainClass>")
   mainClass = sys.argv[1]
   ## alter the mainClass in pom.xml
   alterMainClass(mainClass)  

