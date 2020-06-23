/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include <exception>

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();

	// handle messages from receiving queue
	void checkMessages();

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();

private:
	// download message from emulation net
	static int enqueueWrapper(void *env, char *buff, int size);

	//  ring util functions
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// handles CURD message
	void handleCURDMessage(Message& msg);
		// handles server reply messages
		void handleReplies(Message& msg);
		// handles client request messages
		void handleRequests(Message& msg);

	// handles stabilization message
	void handleStabilizationMessage(Message& msg);

	// clean the outdated replicas in each replica container
	void doKVSGarbageClean();

	// get hased value range of a ceratin replica
	pair<size_t, size_t> getReplicaRange(ReplicaType rt);

	// get iterator of node target distance away
	vector<Node>::iterator traverseNodeItr(int dist);

	// util filter for the replcias' range
  bool rangeFilter(size_t left, size_t right, size_t key);

	// periodically clean timeout user request. 
	void cleanTimedOutRequest();

private:
	// Member representing this member
	Member *memberNode;
	Params *par;
	EmulNet * emulNet;
	Log * log;

	vector<Node> ring;
	vector<Node>::iterator selfItr;
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;

	// DHT replicas
	map<ReplicaType, map<string, string>> mKVS;
	HashTable * ht;
	// on-going client CURD request records
	map<int, tuple<int, int, int, int>> clientRequest;  // (transID: OpType, total, succeed, timestamp)
	map<int, map<string, int>> read_reply;   // (transID: value, count)
};

#endif /* MP2NODE_H_ */
