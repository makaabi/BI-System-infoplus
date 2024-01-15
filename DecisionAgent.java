package projetsma;
import java.util.Iterator;

import jade.core.Agent;
import jade.domain.DFService;
import jade.domain.FIPAException;
import jade.domain.FIPAAgentManagement.DFAgentDescription;
import jade.domain.FIPAAgentManagement.ServiceDescription;
import jade.lang.acl.ACLMessage;
public class DecisionAgent extends Agent {
	String service;
	protected void setup() {
		System.out.println("Hello. My name is "+this.getLocalName());
		System.out.println("Waiting for message ...");
		ACLMessage msg = blockingReceive();
		System.out.println("Received message: "+msg.getContent());
		float msgval=Float.valueOf(msg.getContent());
		
		ACLMessage msg2 = blockingReceive();
		System.out.println("Received message: "+msg2.getContent());
		float msgval2=Float.valueOf(msg2.getContent());
		
		System.out.println("Decision---------------------: ");

		if(msgval2>msgval)
			System.out.println("Augmenter vente clavier: ");
		
	else if(msgval2<=msgval)
		System.out.println("Augmenter vente online: ");
	}
	

	
	
	}
	
