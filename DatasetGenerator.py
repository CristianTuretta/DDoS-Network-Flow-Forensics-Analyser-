import sys
import random
from random import getrandbits
from ipaddress import IPv4Address


# def generate_source_pool(n_member):
# 	pool = set()
# 	for i in range(n_member):
# 		bits = getrandbits(32)  # generates an integer with 32 random bits
# 		addr = IPv4Address(bits)  # instances an IPv4Address object from those bits
# 		pool.add(str(addr))
# 	return pool
#
#
# def generate_source_pool_attackers(n_member):
# 	pool = set()
#
# 	for i in range(n_member):
# 		bits = getrandbits(16)  # generates an integer with 32 random bits
# 		addr = IPv4Address(bits)  # instances an IPv4Address object from those bits
# 		pool.add(str(addr))
# 	return pool


def generate_ip(attacker=False):
	if not attacker:
		bits = getrandbits(32)  # generates an integer with 32 random bits
	else:
		bits = getrandbits(16)

	addr = IPv4Address(bits)  # instances an IPv4Address object from those bits
	return str(addr)



def forge_packet(number, time, source, destination, protocol, length, info):
	pkt = list()
	pkt.append(str(number))  # No.
	pkt.append(str(time))  # Time
	pkt.append(source)  # Source
	pkt.append(destination)  # Destination
	pkt.append(random.choice(list(protocol)))  # Protocol
	pkt.append(str(length))  # Length [16 bit] - contains total length in bytes of UDP datagram (h + data)
	pkt.append(info)  # Info

	return pkt, len(str(CSV_FORMAT.join(pkt)))


# def attack(csv_file, time, target, pkt_size, atk_volume, atk_time_duration, attackers):  # atk_volume is in MB/s
# 	n_packets = atk_volume/pkt_size
# 	count = 1
# 	while count <= n_packets * atk_time_duration:
# 		atk_packet = forge_packet("0", time, attackers, target, {"UDP"}, pkt_size, "attack")
# 		append_to_csv(csv_file, CSV_FORMAT.join(atk_packet))
# 		time += (1.000000 / n_packets) + random.uniform(0.000001, 0.005)
# 		count += 1


def attack(csv_file, time, target, atk_volume, attackerIp):  # atk_volume is in MB/s
	atk_packet, bytes = forge_packet("0", time, attackerIp, target, {"UDP"}, atk_volume, "attack")
	append_to_csv(csv_file, CSV_FORMAT.join(atk_packet))
	return bytes


def append_to_csv(file, line):
	file.write(str(line) + "\n")


DEFAULT_DESTINATION = "8.173.45.67"
CSV_FORMAT = ","
PROTOCOLS = {"UDP"}
DEFAULT_MAX_ATTACK_PACKET_SIZE = 1000000000 # 1 Gbit per packet
DEFAULT_MIN_ATTACK_PACKET_SIZE = 100000000 # 100 Mbit per packet

def generate(file_path, max_dimension):
	csv_file = open(file_path, "a+")

	bytes_written = 0
	id = 1

	time = 0.0


	while bytes_written <= max_dimension:

		# Insert innocent user
		innocentIp = generate_ip(attacker=False)
		packet, linedimension = forge_packet(id, time, innocentIp, DEFAULT_DESTINATION, PROTOCOLS, getrandbits(8), "info")
		append_to_csv(csv_file, CSV_FORMAT.join(packet))

		id +=1
		bytes_written += linedimension
		time += random.uniform(0.000001, 1.0)

		# Insert attacker
		attackerIp = generate_ip(attacker=True)
		linedimension = attack(csv_file, time, DEFAULT_DESTINATION, int(random.uniform(DEFAULT_MIN_ATTACK_PACKET_SIZE, DEFAULT_MAX_ATTACK_PACKET_SIZE)), attackerIp)

		time += random.uniform(0.000001, 1.0)
		bytes_written += linedimension



	csv_file.close()
