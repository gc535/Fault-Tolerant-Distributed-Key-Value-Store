/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
	std::vector<Node> replicas(std::move(findNodes(key)));
	for (int idx = 0; idx < replicas.size(); ++idx)
	{
		Message msg(g_transID, memberNode->addr, CREATE, key, value, static_cast<ReplicaType>(idx+1));
		emulNet->ENsend(&memberNode->addr, replicas[idx].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
	std::vector<Node> replicas(std::move(findNodes(key)));
	for (int idx = 0; idx < replicas.size(); ++idx)
	{
		Message msg(g_transID, memberNode->addr, CREATE, key, "", static_cast<ReplicaType>(idx+1));
		emulNet->ENsend(&memberNode->addr, replicas[idx].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
	std::vector<Node> replicas(std::move(findNodes(key)));
	for (int idx = 0; idx < replicas.size(); ++idx)
	{
		Message msg(g_transID, memberNode->addr, UPDATE, key, value, static_cast<ReplicaType>(idx+1));
		emulNet->ENsend(&memberNode->addr, replicas[idx].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
	std::vector<Node> replicas(std::move(findNodes(key)));
	for (int idx = 0; idx < replicas.size(); ++idx)
	{
		Message msg(g_transID, memberNode->addr, DELETE, key, "", static_cast<ReplicaType>(idx+1));
		emulNet->ENsend(&memberNode->addr, replicas[idx].getAddress(), msg.toString());
	}
	++g_transID;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
	auto& storage = mKVS[replica];
	if (storage.find(key) == storage.end())
	{
		storage[key] = value;
		return true;
	}
	else {return false;}
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
	for (int type = 0; type < 3; type++)
	{
		auto& storage = mKVS[type];
		std::pair<string, string>* reponse = storage.find(key);
		if ( reponse != storage.end() ) 
		{ return reponse->second; }
	}
	return ""; // return empty string if not found
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
	auto& storage = mKVS[replica];
	if (storage.find(key) != storage.end())
	{
		storage[key] = value;
		return true;
	}
	else {return false;}
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
	for (int type = 0; type < 3; type++)
	{
		auto& storage = mKVS[type];
		std::pair<string, string>* reponse = storage.find(key);
		if ( reponse != storage.end() ) 
		{ 
			storage.erase(key);
			return true; 
		}
	}
	return false; // return empty string if not found
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * TODO: 
	 * 1. stablization protocal should has -1 transID
	 * 	a. read is trying to get the replicas value
	 *  b. update is to update replicas
	 *
	 * 2. All CURD message has positive transID, so log them properly
	 * 
	 * 3. Reply message are for counting quorum for that transID. 
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);

		if (msg.transI == -1)
		{
			handleStabilizationMessage(msg);
		}
		else
		{
			handleCURDMessage(msg);
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	std::vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (int i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol()
{	
	// send self's replicas to original holders
	size_t selfHash = hashFunction(memberNode->addr.getAddress());
	for (int type = 1; /*start with SECONDARY*/ 
	     type < 3; /*end with TERTIARY*/ 
			 type++) 
	{
		// find main replicas node
		auto main_replica = traverseNodeItr(-type);
		size_t left = traverseNodeItr(-type - 1)->getHashCode();
		size_t right = main_replica->getHashCode();

		for (auto entry : mKVS.find((ReplicaType)type)->second)
		{
			size_t hashed_key = hashFunction(entry.first);
			if (rangeFilter(left, right, hashFunction(entry.first)))
			{
				Message msg((int)(-1), memberNode->addr, READREPLY, entry.first, entry.second);
				emulNet->ENsend(&memberNode->addr, main_replica->getAddress(), msg.toString());
			}
		}
	}

	// go garbage cleanning for obselet local replicas
	doKVSGarbageClean();

	// send self's primary to two replicas
	for (auto entry : mKVS.find(PRIMARY)->second)
	{
		for (int i = 0; i < hasMyReplicas.size(); ++i)
		{
			Message msg((int)(-1), memberNode->addr, READREPLY, entry.first, entry.second, (ReplicaType)(i+1));
			emulNet->ENsend(&memberNode->addr, hasMyReplicas[i].getAddress(), msg.toString());
		}
	}
}

// clean the outdated replicas in each replica container
void MP2Node::doKVSGarbageClean()
{
	// TODO: drop outof range(outdated replicas)
}

// periodically clean timeout user request. 
void MP2Node::cleanTimedOutRequest()
{
	// TODO: clean map entry timed out (current time - timestamp > tfail)
}

// get hased value range of a ceratin replica
pair<size_t, size_t> MP2Node::getReplicaRange(ReplicaType rt)
{
	auto front = traverseNodeItr((int)rt - 1);
	auto end = traverseNodeItr((int)rt);
	return make_pair(front->nodeHashCode, end->nodeHashCode);
}

// handles CURD message
void MP2Node::handleCURDMessage(Message &msg)
{
	// coordinator received replies, ignore if already reached quorum
	if ( (msg.type == READREPLY || msg.type == REPLY)
	     && clientRequest.find(msg.transID) != clientRequest.end())
	{ handleReplies(msg); }
	else
	{ handleRequests(msg); }
}

// handles CURD reply message
void MP2Node::handleReplies(Message& msg)
{
	auto transcation = clientRequest.find(msg.transID)->second;
	int& type = get<0>(transcation);
	int& count = get<1>(transcation);
	int& succeed = get<2>(transcation);
	if (msg.type == READREPLY)
	{
		count += 1;
		// cached replies
		auto cached_replies = read_reply.find(msg.transID)->second;
		auto recordPtr = cached_replies.find(msg.key);
		if ( recordPtr == cached_replies.end() )
		{ cached_replies.emplace(msg.value, 1); }
		else
		{
			recordPtr->second = recordPtr->second + 1;
			if (recordPtr->second >= 2) // hard code quorum reached
			{ 
				log->logReadSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
    
				// remove quorum cache after op succeed.
				clientRequest.erase(msg.transID);
				read_reply.erase(msg.transID);
				return;
			}
		}
  
		if (count == 3) // all replies, not reach quorum
		{
			log->logReadFail(&memberNode->addr, true, msg.transID, msg.key);
  
			// remove cache after op failure.
			clientRequest.erase(msg.transID);
			read_reply.erase(msg.transID);
		}
   
	}
	else if (msg.type == REPLY)
	{
		count++;
		if (msg.success) { succeed++; }
  
		if (succeed >= 2 || count == 3)  // quorum reached or failed
		{
			switch (type)
			{
			case CREATE:
				if (succeed >= 2) log->logCreateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				else log->logCreateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				break;
    
			case UPDATE:
				if (succeed >= 2) log->logUpdateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				else log->logUpdateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				break;
     
			case DELETE:
				if (succeed >= 2) log->logDeleteSuccess(&memberNode->addr, true, msg.transID, msg.key);
				else log->logDeleteFail(&memberNode->addr, true, msg.transID, msg.key);
				break;
     
			default:
			  throw runtime_error("[ERROR]: Unrecogonized message type.");
				break;
			}
			clientRequest.erase(msg.transID);
		}
	}
}
	
// handles CURD request message
void MP2Node::handleRequests(Message& msg)
{
	// TODO: 
	// For all request, calls the correspinding server side opertaions, get return value
	// 	1. generate msg reply based on return value
	// 	2. log based on the return value
}


// handles stabilization message
void MP2Node::handleStabilizationMessage(Message &msg)
{
	pair<size_t, size_t> range = getReplicaRange(PRIMARY);
	size_t hashed_key = hashFunction(msg.key);
	// entry from replica, to recover primary/original replica
	if (msg.type == READREPLY)
	{
		map<string, string>& storage = mKVS.find(PRIMARY)->second;
		if (rangeFilter(range.first, range.second, hashed_key))
		{
			if (storage.find(msg.key) == storage.end())
			{ storage.emplace(msg.key, msg.value); }
		}
	}
	// primary sent copy to backup in local replica
	else if(msg.type == UPDATE)
	{ mKVS.find(msg.replica)->second.emplace(msg.key, msg.value); }
	else
	{ throw runtime_error("[ERROR]: Unrecogonized stabilization message."); }
	
}

// get iterator of node with a certain distance
vector<Node>::iterator MP2Node::traverseNodeItr(int dist)
{
	vector<Node>::iterator target = selfItr;
	if (dist > 0)
	{
		while (dist > 0)
		{
			target++;
			if (selfItr == ring.end())
			{
				target = ring.begin();
			}
			--dist;
		}
	}
	else if (dist < 0)
	{
		while (dist < 0)
		{
			target--;
			if (selfItr == ring.begin())
			{
				target = ring.end() - 1;
			}
			++dist;
		}
	}
	return target;
}

// util filter for the replcias' range
bool MP2Node::rangeFilter(size_t left, size_t right, size_t key)
{
	if (right < left)
		return ((0 <= key && key <= right) || (key > left));
	else
		return (left <= key && key <= right);
}