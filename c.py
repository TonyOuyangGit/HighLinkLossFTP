import socket
import hashlib
import os
import time
import mmap
import threading
import datetime

# Set address and port
serverAddress = "10.1.1.3"
serverPort = 10000

sequence_size = 1000

# Delimiter
delimiter = "|:|:|";
#repetitive recognition
repeat_flag = True
received_Flag = True
send_Flag = True
start_handle2 = False
run_handle2 = True

start_time = 0
end_time = 0
elasp_time = 0
received_data = [0] * 1500000
file = [0] * 1500000

ack = 0 
result = 0
max_ack_size = 1400
ack_size = 150
old_seqNo = 0
resent_ack_count = 0

file_open = 0
server_address = 0
data_index = 0

def make_ack(array):
    ack = ""

    for item in array:
        ack += str(item) + ","

    return ack


def check_receive(array):
    result = []
    i = 0

    for item in array:
        if item != i:
           result.append(i)
        i += 1

    return result


def handleConnection1():
# Start - Connection initiation
    global data_index
    global received_data
    global result
    global file
    global start_handle2
    global end_time
    global elasp_time
    global received_Flag, send_Flag


    while True:
        
        while True:
            old_seqNo = 0
            data, server = sock.recvfrom(sequence_size + 200)
            seqNo = data.split(delimiter)[0]
            packetLength = data.split(delimiter)[1]
            #print 'received new packet, packet number: %d, length: %d' % (int(seqNo),int(packetLength))
            received_data[int(seqNo)] = int(seqNo)
            file[int(seqNo)] = data.split(delimiter)[2]
            if int(packetLength) < sequence_size:
                data_index = int(seqNo) + 1
                print 'Data received, setting handle2 to be True'
                start_handle2 = True

                #print 'Data receive: {} \n' .format(received_data[:data_index])
                #result = check_receive(received_data[:data_index])
                break
        
        print 'Bulk sent completed, now start receiving reply'
        while True == received_Flag:
            data, server = sock.recvfrom(sequence_size + 200)
            seqNo = data.split(delimiter)[0]
            print "seqNo {}" .format(seqNo)
            if old_seqNo == seqNo:
                print "multiple packet received, dropping it"
                print "old seqNo:{} receive seqNo:{}" .format(old_seqNo, seqNo)
                time.sleep(1)
                continue
            else:
                old_seqNo = seqNo
                received_data[int(seqNo)] = int(seqNo)
                file[int(seqNo)] = data.split(delimiter)[2]
                print "receive new seqNo: {}" .format(seqNo)
                time.sleep(1)
                
        
        end_time = time.time()
        elasp_time = end_time - start_time          
        print 'Transfer completed, start time: {}, stop time: {} total Elapse Time: {}' .format(start_time, end_time, elasp_time)

        print "Closing socket"
        sock.close()
        break
        # f.close()
        
        
def handleConnection2():
# Start - Connection initiation
    global data_index
    global received_data
    global file
    global start_handle2, run_handle2
    global start_time
    global end_time
    global elasp_time
    global received_Flag, send_Flag, max_ack_size, ack_size
    global server_address
    print "handleconnection 2 is up and running"

    
    while True == run_handle2:
        time.sleep(0.05)
        while True == start_handle2:
            #print 'Data receive: {} \n' .format(received_data[:data_index])
            while True == send_Flag:
                print "starting check receive"
                result = check_receive(received_data[:data_index])
                if 0 != len(result):
                    print 'Result: {}' .format(result)
                    print 'Result size: %d' % len(result)
                    ack = make_ack(result)
                    if len(ack) > max_ack_size:
                        print "maximum ack size reached, slicing it"
                        num_packets = len(ack) / ack_size + 1
                        for i in range(num_packets):
                            reply_data = ack[i * ack_size: i * ack_size + ack_size]
                            sent = sock.sendto(reply_data, server_address)
                            print "sliced ack sent, num of ack: %d, size of ack: %d" % (i, len(reply_data))
                            time.sleep(1)
                    else:
                        sent = sock.sendto(ack, server_address)
                        print "sending ack: {}" .format(ack)
                        time.sleep(1)
                else:
                    print "Result: {}" .format(result)
                    print "received completed"
                    send_Flag = False
                    received_Flag = False
        
            for i in range(data_index):
                #print 'now writing i: %d' % i
                file_open.write(file[i])
            print 'file write done, closing file'
            file_open.close() 
            start_handle2 = False
            break
        
  


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.settimeout(1);
server_address = (serverAddress, serverPort)
userInput = raw_input("\nRequested file: ")
message = userInput;
# seqNoFlag = 0
if os.path.exists("r_" + userInput):
    os.remove("r_" + userInput)
    print "Old file deleted!"

file_open = open("r_" + userInput, 'wb')

try:
    # Connection trials
    connection_trials_count=0

    # Send data
    print  'Requesting %s' % message
    sent = sock.sendto(message, server_address)
    start_time = time.time()
except:
    print "send file error"

thread_list = []
connectionThread1 = threading.Thread(target=handleConnection1, args=())
connectionThread1.setDaemon(True)
connectionThread2 = threading.Thread(target=handleConnection2, args=())
connectionThread2.setDaemon(True)
thread_list.append(connectionThread1)
thread_list.append(connectionThread2)
for i in thread_list:
    i.start()
for i in thread_list:
    i.join()


        
    #     # Receive indefinitely
    #     while True:
    #         # Receive response
    #         # print  '\nWaiting to receive..'
    #         try:
    #             data, server = sock.recvfrom(sequence_size + 200)
    #             # print '{}' .format(data)
    #             # Reset failed trials on successful transmission
    #             connection_trials_count=0;
    #         except:
    #             connection_trials_count += 1
    #             if connection_trials_count < 5:
    #                 print "\nConnection time out, retrying"
    #                 continue
    #             else:
    #                 print "\nMaximum connection trials reached, skipping request\n"
    #                 os.remove("r_" + userInput)
    #                 break

    #         connection_trials_count = 0
    #         seqNo = data.split(delimiter)[1]
    #         if old_seqNo == seqNo:
    #             #print "duplicated packet, dropping it"
    #             continue
    #         else:
    #             old_seqNo = seqNo
            
    #         # clientHash = hashlib.sha1(data.split(delimiter)[3]).hexdigest()
    #         # print "Server hash: " + data.split(delimiter)[0]
    #         # print "Client hash: " + clientHash
    #         received_data[int(seqNo)] = int(seqNo)

    #         if True:
    #             packetLength = data.split(delimiter)[2]

    #             if data.split(delimiter)[3] == "FNF":
    #                 print ("Requested file could not be found on the server")
    #                 os.remove("r_" + userInput)
    #             else:
    #                 #f.write(data.split(delimiter)[3]);
    #                 file[int(seqNo)] = data.split(delimiter)[3]

    #             # print "Sequence number: %s\nLength: %s" % (seqNo, packetLength);
    #             # print "Server: %s on port %s" % server;
                
    #         else:
    #             print "Checksum mismatch detected, dropping packet"
    #             print "Server: %s on port %s" % server;
    #             continue;

    #         if int(packetLength) < sequence_size:
    #             data_index = int(seqNo) + 1
    #             #print 'Data receive: {} \n' .format(received_data[:data_index])
    #             result = check_receive(received_data[:data_index])
    #             #print result
                
    #             if (0 != len(result)):
    #                 #print "result length: %d" % len(result)
    #                 if 5 * len(result) > max_ack:
    #                     result = result[:max_ack]
    #                     ack = make_ack(result[:max_ack])
    #                     print "result truncated"
    #                 else:
    #                     ack = make_ack(result)
    #                 print "sending ack size: %d" % len(ack)
    #                 sent = sock.sendto(ack,server)
    #                 resent_ack_count += 1
    #                 sock.settimeout(0.4)
                
    #                 while True:    
    #                     try :
    #                             data, server = sock.recvfrom(sequence_size + 200)
    #                             # print '{}' .format(data)
    #                             # Reset failed trials on successful transmission
    #                             connection_trials_count=0;
    #                     except:
    #                             connection_trials_count += 1
    #                             if connection_trials_count < 2:
    #                                 print "\nConnection time out, retrying"
    #                                 sent = sock.sendto(ack,server)
    #                                 sent = sock.sendto(ack,server)
    #                                 print "sending ack back to server now %d, Count: %d" % (len(ack),resent_ack_count)
    #                                 resent_ack_count += 1
    #                                 continue

    #                     #print "next packet received"
    #                     connection_trials_count = 0 
    #                     if "1" != data:
    #                         repeat_flag = False
    #                         # clientHash = hashlib.sha1(data.split(delimiter)[3]).hexdigest()
    #                         if True:
    #                             packetLength = data.split(delimiter)[2]
    #                             seqNo = data.split(delimiter)[1]
    #                             received_data[int(seqNo)] = int(seqNo)
    #                             file[int(seqNo)] = data.split(delimiter)[3]
    #                             #print "data received : %d " % int(seqNo)

    #                         else:
    #                             print "Checksum mismatch detected, dropping packet"
    #                             continue
    #                     else:
    #                         if True == repeat_flag:
    #                             continue
    #                         else:
    #                             repeat_flag = True
    #                             print "2nd transmission completed"
    #                             #print "Data received {}\n" .format(received_data[:data_index])
    #                             result = check_receive(received_data[:data_index])
    #                             #print "result length: %d" % len(result)
    #                             if 0 != len(result):
    #                                 if 5 * len(result) > max_ack:   
    #                                     result = result[:max_ack]
    #                                     ack = make_ack(result)
    #                                     print "resending ack size %d, count: %d" % (len(ack),resent_ack_count)
    #                                     resent_ack_count += 1
    #                                     #print "ack array: {}" .format(ack)   
                                        
    #                                 else:
    #                                     ack = make_ack(result)
                                        
    #                                 sent = sock.sendto(ack,server)
    #                                 sent = sock.sendto(ack,server)
    #                                 sent = sock.sendto(ack,server)
    #                                 sock.settimeout(0.4)
    #                                 print "sending another ACK back to server"
    #                             else:
    #                                 ack = "done"
    #                                 sent = sock.sendto(ack,server)
    #                                 sent = sock.sendto(ack,server)
    #                                 sent = sock.sendto(ack,server)
    #                                 break
                            

    #             ack = "Done!"
    #             print 'packet are completed'
    #             print 'Requested start time: {}, end time: {}' .format(start_time, time.time())
    #             print "\nElapsed: " + str(time.time() - start_time)
    #             print "data index: %d" % data_index
    #             #print "Received Data: {}" .format(received_data[:data_index])
    #             for i in range(data_index):
    #                 #print i 
    #                 #print "file write: {}" .format(file[i])
    #                 f.write(file[i])
  
    #             fileRead = open("received_" + userInput, 'rb')
    #             data = fileRead.read()
    #             #print "File MD5 is: {}" .format(hashlib.md5(data).hexdigest())
    #             #Do file write here
                  # break 
