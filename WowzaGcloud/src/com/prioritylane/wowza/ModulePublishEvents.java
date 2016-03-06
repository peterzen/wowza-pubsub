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
			sendNotification(mediaCaster, "publish");
		}

		@Override
		public void onStreamStop(IMediaCaster mediaCaster)
		{
			sendNotification(mediaCaster, "unpublish");
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

	
	
	public void onAppStart(IApplicationInstance appInstance)
	{
		this.appInstance = appInstance;
		this.logger = WMSLoggerFactory.getLoggerObj(appInstance);
		this.moduleDebug = getPropertyValueBoolean(PROP_NAME_PREFIX + "Debug", false);
		
		this.topicName = "projects/stagecloud-1210/topics/mediaserver";

		String fullname = appInstance.getApplication().getName() + "/" + appInstance.getName();
		
//		getLogger().info("onAppStart: " + fullname);
		getLogger().info("ModulePublishEvents start publishing to " + this.topicName);

		if (this.logger.isDebugEnabled())
			this.moduleDebug = true;

		if (this.moduleDebug)
			this.logger.info(MODULE_NAME + " DEBUG mode is ON");
		else
			this.logger.info(MODULE_NAME + " DEBUG mode is OFF");

		this.gcloudPublisher = new GcloudPubsubPublisher();
		appInstance.addMediaCasterListener(this.mediaCasterListener);
	}

	
	public void doSomething(IClient client, RequestFunction function, AMFDataList params) {
		getLogger().info("doSomething");
		sendResult(client, params, "Hello Wowza");
	}

	public void sendNotification(IMediaCaster mediaCaster, String message) {
		// TODO Auto-generated method stub
		gcloudPublisher.publishMessage(this.topicName, message);
	}

	public void onAppStop(IApplicationInstance appInstance) {
		this.gcloudPublisher = null;
		String fullname = appInstance.getApplication().getName() + "/" + appInstance.getName();
		getLogger().info("onAppStop: " + fullname);
	}

	public void onConnect(IClient client, RequestFunction function, AMFDataList params) {
		getLogger().info("onConnect: " + client.getClientId());
	}

	public void onConnectAccept(IClient client) {
		getLogger().info("onConnectAccept: " + client.getClientId());
	}

	public void onConnectReject(IClient client) {
		getLogger().info("onConnectReject: " + client.getClientId());
	}

	public void onDisconnect(IClient client) {
		getLogger().info("onDisconnect: " + client.getClientId());
	}

	public void onStreamCreate(IMediaStream stream) {
		getLogger().info("onStreamCreate: " + stream.getSrc());
	}

	public void onStreamDestroy(IMediaStream stream) {
		getLogger().info("onStreamDestroy: " + stream.getSrc());
	}

	public void onCall(String handlerName, IClient client, RequestFunction function, AMFDataList params) {
		getLogger().info("onCall: " + handlerName);
	}

}
