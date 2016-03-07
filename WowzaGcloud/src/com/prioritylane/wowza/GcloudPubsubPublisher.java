package com.prioritylane.wowza;

import com.prioritylane.wowza.PortableConfiguration;
import com.wowza.wms.logging.WMSLogger;
import com.google.api.services.pubsub.model.Topic;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;


public class GcloudPubsubPublisher {
	
	Pubsub pubsub = null;
	WMSLogger logger = null;
	
	public GcloudPubsubPublisher(WMSLogger logger){
		
		this.logger = logger;
		
		try {
			/* the following throws an exception: 
			 * invoke(onStreamCreate): java.lang.reflect.InvocationTargetException|at 
			 * sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)|at 
			 * sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)|at 
			 * sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)|at 
			 * java.lang.reflect.Method.invoke(Method.java:497)|at 
			 * com.wowza.wms.module.ModuleFunction.invoke(ModuleFunction.java:383)|
			 */
			this.pubsub = PortableConfiguration.createPubsubClient();
			
			if(this.pubsub == null){
				getLogger().error("*** could not instantiate pubsub");
			}
		} catch (InvocationTargetException x) {
		    Throwable cause = x.getCause();
		    System.err.format("drinkMe() failed: %s%n", cause.getMessage());

		} catch (Exception e) {
			getLogger().info("**** PubsubClient EXCEPTION");
			e.printStackTrace();
			return;
		}
	}

	public Topic createTopic(String topicName){
		Topic newTopic;
		try {
			newTopic = this.pubsub.projects().topics()
			        .create(topicName, new Topic())
			        .execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		getLogger().info("Created topic " + newTopic.getName());
		return newTopic;
	}
	
	// message = "Hello Cloud Pub/Sub!"
	public void publishMessage(String topicName, String message){
		PubsubMessage pubsubMessage = new PubsubMessage();
		// You need to base64-encode your message with
		// PubsubMessage#encodeData() method.
		try {
			pubsubMessage.encodeData(message.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
		PublishRequest publishRequest =
		        new PublishRequest().setMessages(messages);
		PublishResponse publishResponse;
		try {
			publishResponse = pubsub.projects().topics()
			        .publish(topicName, publishRequest)
			        .execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		List<String> messageIds = publishResponse.getMessageIds();
		if (messageIds != null) {
		    for (String messageId : messageIds) {
		        getLogger().error("messageId: " + messageId);
		    }
		}
	}
	
	private WMSLogger getLogger(){
		return this.logger;
	}
}
