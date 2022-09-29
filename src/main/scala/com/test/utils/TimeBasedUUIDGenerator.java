package com.test.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Random;
import java.util.UUID;

/**
 * Will generate time-based UUID (version 1 UUID). Requires JDK 1.6+
 * 
 */
public class TimeBasedUUIDGenerator {

	public static final Object lock = new Object();

	private static long lastTime;
	private static long clockSequence = 0;
	private static final long hostIdentifier = getHostId();

	/**
	 * Will generate unique time based UUID where the next UUID is always
	 * greater then the previous.
	 */
	public final static UUID generateId() {
		return generateIdFromTimestamp(System.currentTimeMillis());
	}

	public final static UUID generateIdFromTimestamp(long currentTimeMillis) {
		long time;

		synchronized (lock) {
			if (currentTimeMillis > lastTime) {
				lastTime = currentTimeMillis;
				clockSequence = 0;
			} else {
				++clockSequence;
			}
		}

		time = currentTimeMillis;

		// low Time
		time = currentTimeMillis << 32;

		// mid Time
		time |= ((currentTimeMillis & 0xFFFF00000000L) >> 16);

		// hi Time
		time |= 0x1000 | ((currentTimeMillis >> 48) & 0x0FFF);

		long clockSequenceHi = clockSequence;

		clockSequenceHi <<= 48;

		long lsb = clockSequenceHi | hostIdentifier;

		return new UUID(time, lsb);
	}

	private static final long getHostId() {
		long macAddressAsLong = 0;
		try {
			Random random = new Random();
			InetAddress address = InetAddress.getLocalHost();
			NetworkInterface ni = NetworkInterface.getByInetAddress(address);
			if (ni != null) {
				byte[] mac = ni.getHardwareAddress();
				random.nextBytes(mac); // we don't really want to reveal the
										// actual MAC address
				// Converts array of unsigned bytes to an long
				if (mac != null) {
					for (int i = 0; i < mac.length; i++) {
						macAddressAsLong <<= 8;
						macAddressAsLong ^= (long) mac[i] & 0xFF;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return macAddressAsLong;
	}
	
	public static void main(String[] args) {
		UUID id = TimeBasedUUIDGenerator.generateId();
		long s=System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			UUID newId = TimeBasedUUIDGenerator.generateId();
			System.out.println(newId);
		}
		System.out.println(System.currentTimeMillis()-s); 
		
	}
	
}