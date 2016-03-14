#!/bin/sh

export JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"

/Library/WowzaStreamingEngine/java/bin/java  -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044 -Xmx4000M -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -server -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote=true -Dcom.wowza.wms.runmode=standalone -Dcom.wowza.wms.native.base=osx -Dcom.wowza.wms.AppHome=/Library/WowzaStreamingEngine -Dcom.wowza.wms.ConfigURL= -Dcom.wowza.wms.ConfigHome=/Library/WowzaStreamingEngine -cp /Library/WowzaStreamingEngine/bin/wms-bootstrap.jar com.wowza.wms.bootstrap.Bootstrap start
