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


// topicName = "projects/myproject/topics/mytopic"

public class GcloudPubsubPublisher {
	
	Pubsub pubsub = null;
	
	public void initializePubsub(){
		try {
			this.pubsub = PortableConfiguration.createPubsubClient();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}

	public void createTopic(String topicName){
		Topic newTopic;
		try {
			newTopic = this.pubsub.projects().topics()
			        .create(topicName, new Topic())
			        .execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		System.out.println("Created: " + newTopic.getName());
		
	}
	
	// message = "Hello Cloud Pub/Sub!"
	public void publishMessage(String message){
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
			        .publish("projects/myproject/topics/mytopic", publishRequest)
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
