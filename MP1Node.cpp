/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = par->MAX_NNB;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    // add self to self's membership list
    addToMemberList(memberNode->addr, memberNode->heartbeat); 
    // TODO
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   last_dir_ping_addr = Address("0:0"); // dummy address
   wait_area.clear();
   ignore_area.clear();
   reference_membershipTable.clear();
   memberNode->inited = false;
   memberNode->inGroup = false;
   memberNode->bFailed = true;
   return 0;

}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    
    /* Handling MSg */
    // get msg header
    size_t hdr_size = sizeof(MessageHdr);
    MessageHdr msg_hdr;
    memcpy((char *)&msg_hdr, data, hdr_size);

    // get address
    size_t addr_size = sizeof(memberNode->addr.addr);
    Address source_addr;
    memcpy(&source_addr.addr, data + hdr_size, addr_size);
    
    // parse and handle msg
    char* offset =  data + hdr_size + addr_size;
    switch (msg_hdr.msgType)
    {
    case JOINREQ:
    { 
        // add new node to membership list
        long heartbeat;
        memcpy(&heartbeat, offset + 1, sizeof(long));  
        addToMemberList(source_addr, heartbeat); // introcuder add new node in its membership list

        // send JOINREP back with membership list
        std::pair<int, char*> msg_pair = prepareMessage(JOINREP);
        emulNet->ENsend(&memberNode->addr, &source_addr, (char *)msg_pair.second, msg_pair.first);
        free(msg_pair.second);

        break;
    }
    case JOINREP:
    { 
        memberNode->inGroup = true;

        // decode membership list
        int size;
        memcpy(&size, offset, sizeof(int));
        offset += sizeof(int);
        fetchAndUpdateRefMemShipTable(size, offset);
        break;
    }
    case ACK:
    {
        // stamp updated heartbeat 
        long heartbeat;
        memcpy(&heartbeat, offset, sizeof(long));
        updateMemberEntry(source_addr, heartbeat);
        wait_area.erase(source_addr.getAddress());

        break;
    }
    case INDACK:
    { 
        // decode origin address of ping sender
        memcpy(&origin_ping_addr.addr[0], offset, sizeof(int));
        offset += sizeof(int);
	    memcpy(&origin_ping_addr.addr[4], offset, sizeof(short));
        offset += sizeof(short);

        // decode target address of ack receiver
        memcpy(&target_ack_addr.addr[0], offset, sizeof(int));
        offset += sizeof(int);
	    memcpy(&target_ack_addr.addr[4], offset, sizeof(short));
        offset += sizeof(short);

        // decode heartbeat of target ack addr
        memcpy(&target_heartbeat, offset, sizeof(long));

        if (memberNode->addr.getAddress() != origin_ping_addr.getAddress())
        {
            // if self is not origin, send indirect ack back to origin
            std::pair<int, char*> msg_pair = prepareMessage(INDACK);
            emulNet->ENsend(&memberNode->addr, &origin_ping_addr, (char *)msg_pair.second, msg_pair.first);
            free(msg_pair.second);   
        }
        updateMemberEntry(target_ack_addr, target_heartbeat);
        wait_area.erase(target_ack_addr.getAddress());

        break;
    }
    case DIRPING:
    { 
        //sending ack back
        std::pair<int, char*> msg_pair = prepareMessage(ACK);
        emulNet->ENsend(&memberNode->addr, &source_addr, (char *)msg_pair.second, msg_pair.first);
        free(msg_pair.second);

        // decode membership list
        int size;
        memcpy(&size, offset, sizeof(int));
        offset += sizeof(int);
        fetchAndUpdateRefMemShipTable(size, offset);

        break;
    }
    case INDPING:
    {
        // decode origin address of ping sender
        memcpy(&origin_ping_addr.addr[0], offset, sizeof(int));
        offset += sizeof(int);
	    memcpy(&origin_ping_addr.addr[4], offset, sizeof(short));
        offset += sizeof(short);

        // decode target address of ack receiver
        memcpy(&target_ack_addr.addr[0], offset, sizeof(int));
        offset += sizeof(int);
	    memcpy(&target_ack_addr.addr[4], offset, sizeof(short));
        offset += sizeof(short);

        // decode membership list
        int size;
        memcpy(&size, offset, sizeof(int));
        offset += sizeof(int);
        fetchAndUpdateRefMemShipTable(size, offset);

        if (memberNode->addr.getAddress() == target_ack_addr.getAddress())
        {
            // if self is target, send indirect ack back
            std::pair<int, char*> msg_pair = prepareMessage(INDACK);
            emulNet->ENsend(&memberNode->addr, &source_addr, (char *)msg_pair.second, msg_pair.first);
            free(msg_pair.second);   
        }
        else
        {
            // otherwise send indirect ping to target ack address
            std::pair<int, char*> msg_pair = prepareMessage(INDPING);
            emulNet->ENsend(&memberNode->addr, &target_ack_addr, (char *)msg_pair.second, msg_pair.first);
            free(msg_pair.second);   
        }

        break;
    }
    default:
    {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "UNknow message header.");
#endif
        break;
    }
    }

    // delete msg
    delete(data);
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    garbageCleanWaitArea();
    garbageCleanIgnoreArea();
    if (memberListSize > 1)
    { executeSWIMLogic(); }
    updateMemberList();

    // increase node counters
    memberNode->myPos->heartbeat = ++memberNode->heartbeat;
    memberNode->myPos->timestamp = ++memberNode->timeOutCounter;
    
    return;
}

/**
 * FUNCTION NAME: executeSWIMLogic
 *
 * DESCRIPTION: Function execute 2 phases SWIM logic
 */

void MP1Node::executeSWIMLogic()
{
    //std::cout << "SWIM Phase 1 on node: " << memberNode->addr.getAddress() << std::endl;
    /* SWIM Phase 1: select one neighbour for direct ping */
    int memberlistSize = memberNode->memberList.size();
    int pos = rand() % memberlistSize;
    while (memberNode->memberList.data()[pos].id == memberNode->myPos->id
           && memberNode->memberList.data()[pos].port == memberNode->myPos->port)
    { pos = (pos + 1) % memberlistSize; /*increment by 1, wrap around if needed*/ }
    // create target ack addr
    Address target;
    memcpy(&target.addr[0], &(memberNode->memberList.data()[pos].id), sizeof(int));
    memcpy(&target.addr[4], &(memberNode->memberList.data()[pos].port), sizeof(short));
    // generate and send direct ping message
    std::pair<int, char*> dir_ping_msg_pair = prepareMessage(DIRPING);
    emulNet->ENsend(&memberNode->addr, &target, (char *)dir_ping_msg_pair.second, dir_ping_msg_pair.first);
    free(dir_ping_msg_pair.second);
    // add to wait area
    wait_area.emplace(target.getAddress(), memberNode->timeOutCounter);
    
    //std::cout << "SWIM Phase 2 on node: " << memberNode->addr.getAddress() << std::endl;
    /* SWIM Phase 2 */
    if ( last_dir_ping_addr.getAddress() != "0:0" /* cannot be dummy address */ 
        && wait_area.find(last_dir_ping_addr.getAddress()) != wait_area.end() )
    { 
        // if last direct ping not acked, send indirect ping
        memcpy(&origin_ping_addr.addr, &memberNode->addr.addr, sizeof(char[6]));
        memcpy(&target_ack_addr.addr, &last_dir_ping_addr.addr, sizeof(char[6]));
        std::pair<int, char*> indir_ping_msg_pair = prepareMessage(INDPING);

        // select K neighbours
        std::vector<Address> neighbours;
        selectKNeibhbour(memberNode->nnb, neighbours);

        // sending to K neighbours
        for (auto addr : neighbours)
        { emulNet->ENsend(&memberNode->addr, &addr, (char *)indir_ping_msg_pair.second, indir_ping_msg_pair.first); }
        free((void*)indir_ping_msg_pair.second);
    }
    last_dir_ping_addr = target;
}

/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: Function update the real memberList using local reference
 *              membershipTable created based on latests gpssip memssage.
 */
void MP1Node::updateMemberList()
{
    long cur_time = memberNode->timeOutCounter;
    long timeout = memberNode->pingCounter;
    Address entry_addr;
    for (std::vector<MemberListEntry>::iterator memberItr = memberNode->memberList.begin();
         memberItr != memberNode->memberList.end();  /* iterater advance conditionally*/ )
    {
        memcpy(&entry_addr.addr[0], &memberItr->id, sizeof(int));
        memcpy(&entry_addr.addr[4], &memberItr->port, sizeof(short));
        std::unordered_map<std::string, long>::iterator reference_entry;
        reference_entry = reference_membershipTable.find(entry_addr.getAddress());
        
        // if memberList entry has updates in the reference table 
        if (reference_entry != reference_membershipTable.end())
        {
            long reference_heartbeat = reference_entry->second;
            long current_heartbeat = memberItr->heartbeat;
            reference_membershipTable.erase(reference_entry);  // remove processed reference entry

            // update heartbeat if needed
            if (reference_heartbeat > current_heartbeat)
            {
                memberItr->heartbeat =  reference_heartbeat;
                memberItr->timestamp = cur_time;
                memberItr++;
                continue;
            }
        }
        
        // check if inactive for too long, then add to ignore list (await deletion)
        if ( cur_time - memberItr->timestamp >= timeout)
        {
            ignore_area.emplace(entry_addr.getAddress(), memberItr->timestamp);
            memberItr = memberNode->memberList.erase(memberItr);
            log->logNodeRemove(&memberNode->addr, &entry_addr);
        }
        else
        {  memberItr++; }
    }

    //std::cout << "cleanup reference table on node: " << memberNode->addr.getAddress() << std::endl;
    // handle residual entries in the reference map
    for (std::unordered_map<std::string, long>::iterator remain_entry = reference_membershipTable.begin();
         remain_entry != reference_membershipTable.end(); ++remain_entry)
    {
        // if entry not in the ignore area
        if (ignore_area.find(remain_entry->first) == ignore_area.end())
        {
            Address new_peer(remain_entry->first);
            addToMemberList(new_peer, remain_entry->second);
        }
    }
    reference_membershipTable.clear();

    memberListSize = memberNode->memberList.size();
    updateMyPos();
}

/**
 * FUNCTION NAME: garbageCleanWaitArea
 *
 * DESCRIPTION: Function moves obselete item from wait area to ignore area
 */
void MP1Node::garbageCleanWaitArea()
{
    // move timeout ping addr to ignore area waiting to be delete
    // This refers to the 2 * TFail timeout concept in Gossip prevent add back
    long cur_time = memberNode->timeOutCounter;
    long timeout = memberNode->pingCounter;
    for (auto map_itr = wait_area.begin(); map_itr != wait_area.end(); )
    {
        if ( cur_time - map_itr->second >= timeout )
        { 
            ignore_area.emplace(map_itr->first, map_itr->second); 
            map_itr = wait_area.erase(map_itr);
        }
        else
        { ++map_itr; }
    }
}

/**
 * FUNCTION NAME: garbageCleanIgnoreArea
 *
 * DESCRIPTION: Function removes obselete item from ignore area
 */
void MP1Node::garbageCleanIgnoreArea()
{
    long cur_time = memberNode->timeOutCounter;
    long double_timeout = memberNode->pingCounter * 2;
	for (std::unordered_map<std::string, long>::iterator itemItr = ignore_area.begin();
         itemItr != ignore_area.end(); /* advance iterator conditionally*/ )
    {
        if ( cur_time - itemItr->second >= 2 * double_timeout)
        { itemItr = ignore_area.erase(itemItr); }
        else
        { itemItr++; }
    }
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}


/**
 * FUNCTION NAME: addingToMemberList
 *
 * DESCRIPTION: create new member node with given process address and append it to the MemberShipList
 *              This function should only be called once at the begning of during node startup.
 */
void MP1Node::addToMemberList(Address& Addr, long heartbeat)
{
    int id = getID(Addr);
	short port = getPort(Addr);
    long timestamp = memberNode->timeOutCounter;
    MemberListEntry new_memeber(id, port, heartbeat, timestamp);
    memberNode->memberList.emplace_back(new_memeber);
    log->logNodeAdd(&memberNode->addr, &Addr);
    memberListSize = memberNode->memberList.size();  // update memberlist size
    updateMyPos();
}


/* prepare JOINREP msg:  header|membershipTable */
std::pair<int, char*> MP1Node::prepareMessage(MsgTypes hdr_type)
{
    size_t entry_size = sizeof(char[6]) + sizeof(long); // address + heartbeats
    size_t addr_size = sizeof(memberNode->addr.addr);
    size_t hb_size = sizeof(long);
    
    // calculate msg size
    size_t msg_size = sizeof(MessageHdr) + addr_size;
    if ( hdr_type == JOINREP || hdr_type == DIRPING )
    {
        // msg: hdr | addr | memberlist size | memberlist
        msg_size += sizeof(int) + memberListSize * entry_size;
    }
    else if ( hdr_type == INDPING )
    {
        // msg: hdr | addr | origin ping addr | target ack addr | memberlist size | memberlist
        msg_size += addr_size + addr_size + sizeof(int) + memberListSize * entry_size;
    }
    else if ( hdr_type == INDACK)
    {
        // msg: hdr | addr | origin ping addr | target ack addr | heartbeat
        msg_size += 2 * addr_size + hb_size;
    }
    else if ( hdr_type == ACK )
    {
        // msg: hdr | addr | heartbeat
        msg_size += hb_size; 
    }
    else
    {
        // print out all the frames to stderr
	    fprintf(stderr, "Error: Unrecogonized message type.");
	    exit(1);
    }
    
    // prepare msg
    MessageHdr* msg_hdr = (MessageHdr*) malloc(msg_size);
    // write header and advance
    msg_hdr->msgType = hdr_type;
    // write self address
    memcpy((char *) (msg_hdr+1), &memberNode->addr.addr, addr_size);
    char* msg = (char *) (msg_hdr+1) + addr_size;
    
    if ( hdr_type == JOINREP || hdr_type == DIRPING || hdr_type == INDPING)
    {
        if (hdr_type == INDPING)
        {
            // write original ping addr
            memcpy((char *) msg, &origin_ping_addr.addr[0], addr_size);
            msg += addr_size;
            // write target act addr
            memcpy((char *) msg, &target_ack_addr.addr[0], addr_size);
            msg += addr_size;
        }
        // write memberlist size
        memcpy((char *) msg, &memberListSize, sizeof(int));
        msg += sizeof(int);

        // write membership msg body 
        for (auto entry : memberNode->memberList)
        {
            memcpy((char *) msg, (char*) (&entry.id), sizeof(int));
            msg += sizeof(int);
            memcpy((char *) msg, (char*) (&entry.port), sizeof(short));
            msg += sizeof(short);
            memcpy((char *) msg, (char*) (&entry.heartbeat), hb_size);
            msg += hb_size; 
        }
    }
    else if ( hdr_type == INDACK)
    {
        // write original ping addr
        memcpy((char *) msg, &origin_ping_addr.addr[0], addr_size);
        msg += addr_size;
        // write target act addr
        memcpy((char *) msg, &target_ack_addr.addr[0], addr_size);
        msg += addr_size;

        // write heartbeat value
        long heartbeat = target_heartbeat; // using ack target's heartbeat if self is not target
        if ( target_ack_addr.getAddress() == memberNode->addr.getAddress() )
        {
            // if self is target, use self's heartbeat
            heartbeat = memberNode->heartbeat;
        }
        memcpy((char *) msg, (char*) (&heartbeat), hb_size);
        msg += hb_size;
    }
    else if ( hdr_type == ACK)
    {
        memcpy((char *) msg, (char*) (&memberNode->heartbeat), hb_size);
        msg += hb_size;
    }

    return std::pair<int, char*> ((int)msg_size, (char*)msg_hdr);
}

void MP1Node::fetchAndUpdateRefMemShipTable(int size, char* membershipList)
{
    for (int i = 0; i < size; ++i)
    {
        Address temp_addr;
        memcpy(&temp_addr.addr[0], membershipList, sizeof(int));
        membershipList += sizeof(int);
	    memcpy(&temp_addr.addr[4], membershipList, sizeof(short));
        membershipList += sizeof(short);
        long heartbeat;
        memcpy(&heartbeat, membershipList, sizeof(long));
        membershipList += sizeof(long);

        // update/creat entry in reference membershipTable
        auto history = reference_membershipTable.find( temp_addr.getAddress() );
        if ( history != reference_membershipTable.end() )
        {
            // update to latest heartbeat
            if ( heartbeat > history->second )
            { history->second = heartbeat; } 
        }
        else
        {
            reference_membershipTable.emplace(temp_addr.getAddress(), heartbeat);
        }
    }
}

void MP1Node::selectKNeibhbour(int k, std::vector<Address>& cand_addrs)
{
    int cur_size = memberNode->memberList.size();
    int gen[cur_size];
    for(int i = 0; i < cur_size; ++i)
    {  gen[i] = i; }

    // shuffle
    for(int i = cur_size-1; i > 0; --i)
    {
        //get swap index
        int j = rand()%i;
        //swap p[i] with p[j]
        int temp = gen[i];
        gen[i] = gen[j];
        gen[j] = temp;
    }

    k = k < cur_size ? k : cur_size - 1;
    auto member_array = memberNode->memberList.data();
    for(int i = 0; i <k; ++i)
    {
        auto cand_itr = &member_array[gen[i]];
        // exclude self
        if ( cand_itr->id != memberNode->myPos->id 
            || cand_itr->port != memberNode->myPos->port )
        {  
            // create address obj
            Address cand_addr;
            memcpy(&cand_addr.addr[0], &cand_itr->id, sizeof(int));
	        memcpy(&cand_addr.addr[4], &cand_itr->port, sizeof(short));
            cand_addrs.push_back(cand_addr); 
        }  
    }
}


/* update entry in membershipList with highest heartbest, create new entry if not exist */
void MP1Node::updateMemberEntry(Address& addr, long heartbeat )
{
    int id = getID(addr);
	short port = getPort(addr);
    for (auto entry : memberNode->memberList)
    {
        // peer exists, update heartbeat when needed
        if (entry.id == id && entry.port == port)
        { 
            if (entry.heartbeat < heartbeat) 
            { entry.heartbeat = heartbeat; }

            return;
        }
    }

    // peer not exist, add to membership List
    addToMemberList(addr, heartbeat);
}

/* update iterator to self in the memberlist vector */
void MP1Node::updateMyPos()
{
    for (std::vector<MemberListEntry>::iterator memberItr = memberNode->memberList.begin();
         memberItr != memberNode->memberList.end(); ++memberItr)
    {
        if( memberItr->id == getID(memberNode->addr) && memberItr->port == getPort(memberNode->addr) )
        { memberNode->myPos = memberItr; }
    }
}

/* 
   Get the latest heartbeat of member (addr), this will always return a 
   valid value becuase updateMemberEntry() is already executed a step ahead.
*/
long MP1Node::getMemberHeartbeat(Address& addr)
{
    int id = getID(addr);
	short port = getPort(addr);
    for (auto entry : memberNode->memberList)
    {
        if (entry.id == id && entry.port == port)
        { return entry.heartbeat; }
    }
    return -1;
}

int MP1Node::getID(Address& addr)
{
    int id;
    memcpy(&id, &addr.addr[0], sizeof(int));
    return id;
}

short MP1Node::getPort(Address& addr)
{
    short port;
    memcpy(&port, &addr.addr[4], sizeof(short));
    return port;
}