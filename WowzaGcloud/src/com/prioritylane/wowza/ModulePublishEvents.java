package com.prioritylane.wowza;

import com.wowza.wms.amf.*;
import com.wowza.wms.request.*;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.application.WMSProperties;
import com.wowza.wms.client.IClient;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.mediacaster.IMediaCaster;
import com.wowza.wms.mediacaster.MediaCasterNotifyBase;
import com.wowza.wms.module.ModuleBase;
import com.wowza.wms.server.Server;
import com.wowza.wms.stream.IMediaStream;

import java.io.IOException;

import com.prioritylane.wowza.GcloudPubsubPublisher;

public class ModulePublishEvents extends ModuleBase {

	// static constants
	private static String PROP_NAME_PREFIX = "eventpub";
	private static String MODULE_NAME = "EventPublisher";

	public static boolean serverDebug = false;
	private static WMSProperties serverProps = Server.getInstance().getProperties();

	// instance variables
	private WMSLogger logger = null;
	private boolean moduleDebug = false;
	private IApplicationInstance appInstance = null;
	private MediaCasterListener mediaCasterListener = new MediaCasterListener();
	
	private GcloudPubsubPublisher gcloudPublisher = null;
	private String topicName = null;
	
	class MediaCasterListener extends MediaCasterNotifyBase
	{
		@Override
		public void onStreamStart(IMediaCaster mediaCaster)
		{
			getLogger().info("**** onStreamStart");
			publishMessage("onStreamStart");
		}

		@Override
		public void onStreamStop(IMediaCaster mediaCaster)
		{
			getLogger().info("**** onStreamStop");
			publishMessage("onStreamStop");
		}
	}
	


	static
	{
		serverDebug = serverProps.getPropertyBoolean(PROP_NAME_PREFIX + "Debug", false);
		if (WMSLoggerFactory.getLogger(ModulePublishEvents.class).isDebugEnabled())
			serverDebug = true;

		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			@Override
			public void run()
			{
					// TODO 
					// shut down gcloud publisher
					if (serverDebug)
						WMSLoggerFactory.getLogger(getClass()).info(MODULE_NAME + " Runtime.getRuntime().addShutdownHook");
			}
		});
	}

	private boolean getPropertyValueBoolean(String key, boolean defaultValue)
	{
		boolean value = serverProps.getPropertyBoolean(key, defaultValue);
		value = this.appInstance.getProperties().getPropertyBoolean(key, value);
		return value;
	}

	private String getPropertyValueStr(String key)
	{
		String value = serverProps.getPropertyStr(key);
		value = this.appInstance.getProperties().getPropertyStr(key, value);
		return value;
	}

	
	
	public void onAppStart(IApplicationInstance appInstance) throws IOException
	{
		this.appInstance = appInstance;
		this.logger = WMSLoggerFactory.getLoggerObj(appInstance);
		this.moduleDebug = getPropertyValueBoolean(PROP_NAME_PREFIX + "Debug", false);
		
//		String fullname = appInstance.getApplication().getName() + "/" + appInstance.getName();
		
//		getLogger().info("onAppStart: " + fullname);
		if (this.logger.isDebugEnabled())
			this.moduleDebug = true;

		if (this.moduleDebug)
			this.logger.info(MODULE_NAME + " DEBUG mode is ON");
		else
			this.logger.info(MODULE_NAME + " DEBUG mode is OFF");

		appInstance.addMediaCasterListener(this.mediaCasterListener);
		
		// TODO 
		// this should be configurable – pulled in through getPropertyValueStr()
		topicName = "projects/stagecloud-1210/topics/mediaserver";
		
		gcloudPublisher = new GcloudPubsubPublisher(getLogger());
	}

		
	public void publishMessage(String message) {
		gcloudPublisher.publishMessage(topicName, message);
	}

	/*
	 * TODO - implement me
	 * 
	 * attributes should contain the data from the context where the event
	 * occured (IMediaStream, IMediaCaster etc)
	 * 
	 public void publishMessage(String message, SomeGenericHashType attributes) {
		gcloudPublisher.publishMessage(topicName, message, attributes);
	 }
	 */


	public void onAppStop(IApplicationInstance appInstance) {
		// TODO
		// properly disconnect/tear down pubsub connection
		gcloudPublisher = null;
		String fullname = appInstance.getApplication().getName() + "/" + appInstance.getName();
		getLogger().info("**** onAppStop: " + fullname);
	}

	public void onConnect(IClient client, RequestFunction function, AMFDataList params) {
		getLogger().info("**** onConnect: " + client.getClientId());
	}

	public void onConnectAccept(IClient client) {
		getLogger().info("**** onConnectAccept: " + client.getClientId());
	}

	public void onConnectReject(IClient client) {
		getLogger().info("**** onConnectReject: " + client.getClientId());
	}

	public void onDisconnect(IClient client) {
		getLogger().info("**** onDisconnect: " + client.getClientId());
	}

	public void onStreamCreate(IMediaStream stream) {
		getLogger().info("**** onStreamCreate: " + stream.getSrc());
		// TODO
		// pass 'stream' to publishMessage()
		publishMessage("onStreamCreate");
	}

	public void onStreamDestroy(IMediaStream stream) {
		getLogger().info("**** onStreamDestroy: " + stream.getSrc());
		// TODO
		// pass 'stream' to publishMessage()
		publishMessage("onStreamDestroy");
	}

	public void onCall(String handlerName, IClient client, RequestFunction function, AMFDataList params) {
		getLogger().info("**** onCall: " + handlerName);
	}

}
