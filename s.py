
import socket
import threading
import hashlib
import time
import datetime
import random
import mmap

sequence_size = 1000

# Set address and port
serverAddress = "10.1.1.3"
serverPort = 10000

missing_packets = 0


# Delimiter
delimiter = "|:|:|";

# Seq number flag
seqFlag = 0
receive_Flag = True
send_Flag = False

#file memory mm
mm = 0
# Packet class definition
class packet():
    length = 0;
    seqNo = 0;
    msg = 0;
    def ch_seqNo(self, seq_Num):
        self.seqNo = seq_Num

    def make(self, data):
        self.msg = data
        self.length = str(len(data))
        #print "Length: %s\nSequence number: %s" % (self.length, self.seqNo)

pkt_s1 = packet()
pkt_s2 = packet()
pkt_s3 = packet()

def file_read(data):
    # Read requested file
    try:
        print "Opening file %s" % data
        with open(str(data), "r+b") as f:
        # memory-map the file, size 0 means whole file
            mm = mmap.mmap(f.fileno(), 0)
        # return mm
        return mm

    except:
        msg="FNF"
        print "Requested file could not be found"
        return
# Connection handler
# Connection handler
def handleConnection1(address, mm):
    server1_packet_count = 0
    packet_count = 0
    # UDP datagram size is 1000 byte 
    # time.sleep(0.5)
    # print "Request started at: " + str(datetime.datetime.utcnow())
        
    threadSock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    threadSock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    num_packets = len(mm) / sequence_size + 1

    for i in range(num_packets):
        msg = mm[i * sequence_size:i * sequence_size + sequence_size]
        pkt_s1.ch_seqNo(i)
        pkt_s1.make(msg)
        finalPacket_s1 = str(pkt_s1.seqNo) + delimiter + str(pkt_s1.length) + delimiter + pkt_s1.msg

        sent = threadSock1.sendto(finalPacket_s1, address)
        # sent = threadSock1.sendto(finalPacket, address)

        server1_packet_count += 1
        # print "sent"
        # threadSock.settimeout(2)

    print "total sent packets: %s" % str(server1_packet_count)


def handleConnection2(address, mm):
    global send_Flag, receive_Flag, missing_packets
    server2_packet_count = 0
    packet_count = 0
    # UDP datagram size is 1000 byte 
    # time.sleep(0.5)
    # print "Request started at: " + str(datetime.datetime.utcnow())
        
    threadSock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    threadSock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    num_packets = len(mm) / sequence_size + 1

    for i in range(num_packets):
        msg = mm[i * sequence_size:i * sequence_size + sequence_size]
        pkt_s2.ch_seqNo(i)
        pkt_s2.make(msg)
        finalPacket_s2 = str(pkt_s2.seqNo) + delimiter + str(pkt_s2.length) + delimiter + pkt_s2.msg

        sent = threadSock2.sendto(finalPacket_s2, address)
        # sent = threadSock1.sendto(finalPacket, address)

        server2_packet_count += 1

    print "total sent packets: %s" % str(server2_packet_count)
    print "start sending ACK:"
    while True == receive_Flag:
        time.sleep(0.1)
        while True == send_Flag:
            reply_array = missing_packets
            
            for packet in reply_array:
                #print "missing packets: %s" % packet
                #pkt = packet()
                packet_num = int(packet)
                mm = data[packet_num * sequence_size:packet_num * sequence_size + sequence_size]
                pkt_s2.ch_seqNo(packet_num)
                pkt_s2.make(msg)
                finalPacket_s2 = str(pkt_s2.seqNo) + delimiter + str(pkt_s2.length) + delimiter + pkt_s2.msg
                print "sending data packet: %s" % str(pkt_s2.seqNo)
                sent = threadSock2.sendto(finalPacket_s2, address)
                print "sending data packet: %s" % str(pkt_s2.seqNo)
                sent = threadSock2.sendto(finalPacket_s2, address)
                time.sleep(1)
            print 'all packets have been resent'
    


def handleConnection3(address, mm):
    global missing_packets, send_Flag, receive_Flag, ack
    server3_packet_count = 0
    packet_count = 0
    # UDP datagram size is 1000 byte 
    # time.sleep(0.5)
    # print "Request started at: " + str(datetime.datetime.utcnow())
        
    threadSock3 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    threadSock3.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    threadSock3.settimeout(0.5)
    num_packets = len(mm) / sequence_size + 1

    for i in range(num_packets):
        msg = mm[i * sequence_size:i * sequence_size + sequence_size]
        pkt_s3.ch_seqNo(i)
        pkt_s3.make(msg)
        finalPacket_s3 = str(pkt_s3.seqNo) + delimiter + str(pkt_s3.length) + delimiter + pkt_s3.msg

        sent = threadSock3.sendto(finalPacket_s3, address)
        # sent = threadSock1.sendto(finalPacket, address)

        server3_packet_count += 1

    print "total sent packets: %s" % str(server3_packet_count)
    print "start receiving ACKs"
    while True == receive_Flag:
        time.sleep(0.1)
        while True == receive_Flag:
            ack, address = sock.recvfrom(2000)
            print "received"
            if 'done' != ack:
                ack = ack.split(",")
                if len(ack) > 1:
                    del ack[-1]
                print 'receive missing packets: {}' .format(ack)
                time.sleep(1)
                missing_packets = ack
                send_Flag = True
            else:
                print 'done is received, stop server'
                receive_Flag = False

                
            



    # missing_packets = ack.split(",")
    # del missing_packets[-1]
    # #print "received ACKs: {}" .format(missing_packets)

    # while missing_packets[0] != "done":
    #     for packet in missing_packets:
    #         #print "missing packets: %s" % packet
    #         #pkt = packet()
    #         packet_num = int(packet)
    #         msg = data[packet_num * packet_size:packet_num * packet_size + packet_size]
    #         pkt.ch_seqNo(packet_num)
    #         pkt.make(msg)
    #         finalPacket = str(pkt.checksum) + delimiter + str(pkt.seqNo) + delimiter + str(
    #                 pkt.length) + delimiter + pkt.msg
    #         #print "sending data packet: %s" % str(pkt.seqNo)
    #         sent = threadSock.sendto(finalPacket, address)

    #     #print "all resent packets has been resent"
    #     finalPacket = "1"
    #     sent = threadSock.sendto(finalPacket, address)  
    #     sent = threadSock.sendto(finalPacket, address)
    #     sent = threadSock.sendto(finalPacket, address)
    #     sock.settimeout(0.4)
    #     try:
    #         [ack, address] = threadSock.recvfrom(3000)

    #     except:
    #         print "Can not get the ACK!!"
        
    #     missing_packets = ack.split(",")
        
    #     if len(missing_packets) > 1 :
    #         del missing_packets[-1]
        
    #     #print "received ACKs: {}" .format(missing_packets)

    # print "successfully transmitted!"
    # print "\nElapsed: " + str(time.time() - start_time)





# Start - Connection initiation
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# Bind the socket to the port
server_address = (serverAddress, serverPort)
print  'Starting up on %s port %s' % server_address
sock.bind(server_address)

# Listening for requests indefinitely
while True:
    print  'Waiting to receive message'
    data, address = sock.recvfrom(600)
    thread_list = []
    mm = file_read(data)
    connectionThread1 = threading.Thread(target=handleConnection1, args=(address, mm))
    connectionThread1.setDaemon(True)
    connectionThread2 = threading.Thread(target=handleConnection2, args=(address, mm))
    connectionThread2.setDaemon(True)
    connectionThread3 = threading.Thread(target=handleConnection3, args=(address, mm))
    connectionThread3.setDaemon(True)
    thread_list.append(connectionThread1)
    thread_list.append(connectionThread2)
    thread_list.append(connectionThread3)
    for i in thread_list:
        i.start()
    for i in thread_list:
        i.join()


    print  'Received %s bytes from %s' % (len(data), address)



