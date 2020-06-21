/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"
 
 #include <unordered_map> 
 #include <unordered_set>

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
		
		ACK,
		INDACK,
		DIRACK, 
		
		/* Msg for SWIM failture detection */
		DIRPING,  // direct ping 
		INDPING,  // indirect ping 
		/* End of SWIM failure detection msg */

    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];

	// SWIM related
	Address origin_ping_addr;
	Address target_ack_addr;
	Address last_dir_ping_addr = Address("0:0"); /* dummy address */
	long target_heartbeat;
	std::unordered_map<std::string, long> wait_area;
	std::unordered_map<std::string, long> ignore_area;

	// gosspi related
	int memberListSize = 0; // 0 init
	std::unordered_map<std::string, long> reference_membershipTable; // addr_str, heartbeat

	//TODO:
	// 		2. Should make sure MembershipList consistent with local map	

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();

	// SWIM related
	void addToMemberList(Address& addr, long heartbeat);
	/* execute 2 phases SWIM pinging logic */
	void executeSWIMLogic();
	/* update memberTable and its size after membershipList updated*/
	void updateMemberList(); 
	/* moves obselet item from wait area to ignore area */
	void garbageCleanWaitArea();
  /* Function removes obselete item from ignore area */
	void garbageCleanIgnoreArea();
	/* pack current node's membership list into a char* msg */
	std::pair<int, char*> prepareMessage(MsgTypes hdr_type);
	/* update loca Reference memShipTable before updating actual MembershipList in node */
	void fetchAndUpdateRefMemShipTable(int size, char* membershipList);
	/* select k number of members from the list */
	void selectKNeibhbour(int k, std::vector<Address>& cand_addrs);
	/* stamp local entry with new heartbeat */
	void updateMemberEntry(Address& addr, long heartbeat);
	/* update myPos after table update */
	void updateMyPos();
	/* get latest heartbeat of member with (addr) */
	long getMemberHeartbeat(Address& addr);
	
	/* get ID/Port from address */
	int getID(Address& addr);
	short getPort(Address& addr);

};

#endif /* _MP1NODE_H_ */
