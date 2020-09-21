#!/usr/bin/python
# -*- coding: UTF-8 -*-
 
from xml.etree.ElementTree import parse

def alterMainClass(mainClassName):
   # get the root of the xml tree
   tree = parse("pom.xml")
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
   tree.write("pom.xml")


if __name__ == "__main__":
   alterMainClass("cn.edu.neu.tiger.RecPipelineWithPrometheus333")   
