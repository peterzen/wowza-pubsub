package com.prioritylane.wowza;

import com.prioritylane.wowza.PortableConfiguration;

import com.google.api.services.pubsub.model.Topic;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;


public class GcloudPubsubPublisher {
	
	Pubsub pubsub = null;
	
	public GcloudPubsubPublisher(){
		try {
			this.pubsub = PortableConfiguration.createPubsubClient();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		System.out.println("Created: " + newTopic.getName());
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
		        System.out.println("messageId: " + messageId);
		    }
		}
	}
}
