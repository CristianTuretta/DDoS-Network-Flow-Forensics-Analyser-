import sys
import random
from random import getrandbits
from ipaddress import IPv4Address


def generate_source_pool(n_member):
	pool = set()
	for i in range(n_member):
		bits = getrandbits(32)  # generates an integer with 32 random bits
		addr = IPv4Address(bits)  # instances an IPv4Address object from those bits
		pool.add(str(addr))
	return pool


def generate_source_pool_attackers(n_member):
	pool = set()

	for i in range(n_member):
		bits = getrandbits(16)  # generates an integer with 32 random bits
		addr = IPv4Address(bits)  # instances an IPv4Address object from those bits
		pool.add(str(addr))
	return pool


def forge_packet(number, time, source, destination, protocol, length, info):
	pkt = list()
	pkt.append(str(number))  # No.
	pkt.append(str(time))  # Time
	pkt.append(random.choice(list(source)))  # Source
	pkt.append(destination)  # Destination
	pkt.append(random.choice(list(protocol)))  # Protocol
	pkt.append(str(length))  # Length [16 bit] - contains total length in bytes of UDP datagram (h + data)
	pkt.append(info)  # Info

	return pkt


def attack(csv_file, time, target, pkt_size, atk_volume, atk_time_duration, attackers):  # atk_volume is in MB/s
	n_packets = atk_volume/pkt_size
	count = 1
	while count <= n_packets * atk_time_duration:
		atk_packet = forge_packet("0", time, attackers, target, {"UDP"}, pkt_size, "attack")
		append_to_csv(csv_file, CSV_FORMAT.join(atk_packet))
		time += (1.000000 / n_packets) + random.uniform(0.000001, 0.005)
		count += 1


def append_to_csv(file, line):
	file.write(str(line) + "\n")


DEFAULT_DESTINATION = "8.173.45.67"
CSV_FORMAT = ","
PROTOCOLS = {"UDP"}


def generate(file_path, n_members, records_length, n_attackers, atk_volume, atk_duration):
	csv_file = open(file_path, "a+")

	no = 1
	normal_usr_pool = generate_source_pool(n_member=n_members)
	time = 0.0

	attack_pool = generate_source_pool_attackers(n_attackers)

	while no <= records_length:
		packet = forge_packet(no, time, normal_usr_pool, DEFAULT_DESTINATION, PROTOCOLS, getrandbits(16), "info")
		append_to_csv(csv_file, CSV_FORMAT.join(packet))

		no += 1
		time += random.uniform(0.000001, 1.0)

	if attack_pool:
		attack(csv_file, time, DEFAULT_DESTINATION, 1000, atk_volume, atk_duration, attack_pool)

	csv_file.close()
