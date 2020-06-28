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
#include <unordered_map>

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

	/* client side CRUD APIs */
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	/* server side APIs */
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// receive messages from Emulnet
	bool recvLoop();
	// handle messages from receiving queue
	void checkMessages();
	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	

	~MP2Node();

private:
	/* ring stucture related functions */
	vector<Node> getMembershipList();
	// re-populate the hasMyReplicas and haveReplicasOf container with new ring
	void findNeighbors();

	/* client request related */
	void handleCURDMessage(Message& msg);
	// handles server reply messages
	void handleReplies(Message& msg);
	// handles client request messages
	void handleRequests(Message& msg);
	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	/* stabilization protocol related */
	void stabilizationProtocol();
	// handles stabilization message
	void handleStabilizationMessage(Message& msg);
	// clean the out-of-range replicas in each replica container
	void doKVSGarbageClean();
	// periodically clean timeout user request. 
	void cleanTimedOutRequest();

	/* helper functions */
	size_t hashFunction(string key);
	// download message from emulation net
	static int enqueueWrapper(void *env, char *buff, int size);
	// get hased value range of a node
	pair<size_t, size_t> getNodeRange(int dist);
	// get iterator of node target distance away
	vector<Node>::iterator traverseNodeItr(int dist);
	// util filter for the replcias' range
  bool rangeFilter(size_t left, size_t right, size_t key);

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
