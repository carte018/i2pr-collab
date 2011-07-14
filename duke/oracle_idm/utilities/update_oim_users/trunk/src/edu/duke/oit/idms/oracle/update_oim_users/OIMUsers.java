package edu.duke.oit.idms.oracle.update_oim_users;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class OIMUsers {

	private HashMap<String, Long> users;
	private File file;
	
	public OIMUsers () {
		users = new HashMap<String, Long>();
		file = null;
	}
	
	public void getNetIDsFromFile(String fileName) {
		if (file == null) {
			file = new File(fileName);
			if (file != null) {
				BufferedReader reader;
				try {
					reader = new BufferedReader(new FileReader(file));
					String line = null;

					// begin reading lines from file
					while((line = reader.readLine()) != null) {
						users.put(line, 0L);
					}
				} catch (IOException e) {
					System.out.println("Could not read NetIDs from file: " + fileName);
					e.printStackTrace();
				}
			}
		}
	}
	
	public void setKeyForUser(String netid, long userKey) {
		users.put(netid, userKey);
	}
	
	public long getKeyForUser(String netid) {
		return (users.get(netid));
	}
	
	public void addNetID(String netid) {
		addNetID(netid, 0L);
	}
	
	public void addNetID(String netid, Long userKey) {
		users.put(netid, userKey);
	}
	
	public int getUserCount() {
		return users.size();
	}
	
	public HashMap<String, Long> getUsers() {
		return users;
	}
	
	public Set<String> getNetIDs() {
		Set<String> netids = users.keySet();
		return Collections.unmodifiableSet(netids);
	}
	
	public Long [] getUserKeys() {
		Collection<Long> keys =users.values();
		Iterator<Long> keyIterator = keys.iterator();
		Set<Long> validKeys = new HashSet<Long>();
		while (keyIterator.hasNext()) {
			long key = keyIterator.next();
			if (key > 0)
				validKeys.add(key);
		}
		return validKeys.toArray(new Long[validKeys.size()]);
	}

}
