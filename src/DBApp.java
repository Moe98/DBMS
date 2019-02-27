import java.io.*;

import java.util.*;
import java.util.concurrent.SynchronousQueue;

public class DBApp {

	static int maxTuplesPerPage = 3;
	static ArrayList<Table> tables;
	static String deprecated = "$";

	public static void init() {
		tables = new ArrayList();

		// initialize tables array using metadata
		//
	}

	public static ArrayList<Pair> search(String strTableName, Hashtable<String, Object> htblColNameValue) {
		ArrayList<Pair> pageIndex = new ArrayList<>();
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		for (String colName : htblColNameValue.keySet()) {
			Object value = htblColNameValue.get(colName);
			tuple[idx++] = value;
		}
		for (String path : paths) {
			if (path.startsWith(strTableName)) {
				last = path;
				counter++;
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size(); i++) {
					o = (Object[]) (v.get(i));
					boolean equals = true;
					for (int j = 0; j < o.length && j < tuple.length; j++) {
						// System.out.println(o[j] instanceof String);
						if (!o[j].equals(tuple[j]) && !(tuple[j] instanceof String && tuple[j].equals(deprecated)))
							equals = false;
					}
					if (equals)
						pageIndex.add(new Pair(path, i));
				}
			}
		}
		return pageIndex;
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		// updates metadata.CSV

	}
	// public void createBitmapIndex(String strTableName,String strColName) throws
	// DBAppException{
	//
	// }

	public static Table getTable(String strTableName) {
		// method not implemented yet !!!
		return null;
	}

	public static void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException {
		// Table table=getTable(strTableName);
		// table.insertIntoTable(htblColNameValue);
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		for (String colName : htblColNameValue.keySet()) {
			Object value = htblColNameValue.get(colName);
			tuple[idx++] = value;
		}
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String last = null;
		for (String path : paths) {
			if (path.startsWith(strTableName)) {
				last = path;
				counter++;
			}
		}
		// refactor
		if (last == null || getNumberOfTuples("pages/" + last).size() == maxTuplesPerPage) {
			// create new page
			++counter;

			Vector<Object> v = new Vector();
			v.add(tuple);
			try {
				FileOutputStream fileOut = new FileOutputStream("pages/" + strTableName + "_" + counter);
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(v);
				out.close();
				fileOut.close();

			} catch (IOException i) {
				i.printStackTrace();
			}
		} else {
			Vector<Object> v = getNumberOfTuples("pages/" + last);
			v.add(tuple);
			try {
				FileOutputStream fileOut = new FileOutputStream("pages/" + last);
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(v);
				out.close();
				fileOut.close();

			} catch (IOException i) {
				i.printStackTrace();
			}
		}
		// pushDown(strTableName, htblColNameValue);

	}

	static void pushDown(int where, String strTableName, Hashtable<String, Object> htblColNameValue) {
		where--;
		Object[] tuple = new Object[htblColNameValue.size()];
		int pushed = 0;
		int idx = 0;
		for (String colName : htblColNameValue.keySet()) {
			Object value = htblColNameValue.get(colName);
			tuple[idx++] = value;
		}
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String curPath = null;

		int remainingFlag = 0;
		int enteries = 0;
		for (String path : paths) {

			if (path.startsWith(strTableName)) {
				curPath = path;

				Vector<Object> newV = new Vector();
				Vector<Object> oldV = getNumberOfTuples("pages/" + curPath);

				if (remainingFlag == 1) {
					newV.add(tuple);
					remainingFlag = 0;
				}

				if (pushed == 0)
					if (enteries + oldV.size() < where) {
						enteries += oldV.size();
						counter++;
						continue;
					} else {

						int i = 0;
						while (true) {

							if (enteries + i == where) {
								newV.add(tuple);
								break;
							}
							newV.add(oldV.remove(0));

							i++;
						}
						pushed = 1;
					}

				newV.addAll(oldV);

				if (newV.size() > maxTuplesPerPage) {
					tuple = (Object[]) newV.remove(newV.size() - 1);
					remainingFlag = 1;
				} else {
					remainingFlag = 0;
				}

				try {
					FileOutputStream fileOut = new FileOutputStream("pages/" + curPath);
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(newV);
					out.close();
					fileOut.close();

				} catch (IOException i) {
					i.printStackTrace();
				}

				counter++;
			}
		}

		if (pushed == 0)
			remainingFlag = 1;
		if (remainingFlag == 1) {
			++counter;
			Vector<Object> v = new Vector();
			v.add(tuple);
			try {
				FileOutputStream fileOut = new FileOutputStream("pages/" + strTableName + "_" + counter);
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(v);
				out.close();
				fileOut.close();

			} catch (IOException i) {
				i.printStackTrace();
			}
		}

	}

	static void readTables(String strTableName) {
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String curPath = null;
		for (String path : paths) {
			if (path.startsWith(strTableName)) {
				curPath = path;
				Vector<Object> curV = getNumberOfTuples("pages/" + curPath);
				for (int i = 0; i < curV.size(); i++) {
					Object[] v = (Object[]) curV.get(i);
					for (Object o : v) {
						System.out.print(o.toString() + " ");
					}
					System.out.println();
				}
				System.out.println("----------new page---------");
			}
		}
	}

	static Vector<Object> getNumberOfTuples(String className) {
		Vector<Object> v;
		try {
			FileInputStream fileIn = new FileInputStream(className);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			v = (Vector) in.readObject();
			in.close();
			fileIn.close();
			return v;
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {
			System.out.println("class not found");
			c.printStackTrace();
			return null;
		}
	}

	public static void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		Table table = getTable(strTableName);
		table.updateTable(strKey, htblColNameValue);
		/*
		 * we cant just call delete then update
		 */

	}

	public static void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		// Table table = getTable(strTableName);
		// table.deleteFromTable(htblColNameValue);
		// ArrayList<Pair> toBeDeleted = search(strTableName, htblColNameValue);
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		for (String colName : htblColNameValue.keySet()) {
			Object value = htblColNameValue.get(colName);
			tuple[idx++] = value;
		}
		for (String path : paths) {
			if (path.startsWith(strTableName)) {
				last = path;
				counter++;
				System.out.println("Path: " + path);
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size(); i++) {
					o = (Object[]) (v.get(i));
					boolean equals = true;
					for (int j = 0; j < o.length; j++)
						if (!o[j].equals(tuple[j]) && !(tuple[j] instanceof String && tuple[j].equals(deprecated)))
							equals = false;
					if (equals) {
						// System.out.println("" + v.size() + " i:" + i);
						v.remove(i);
						i = 0;
					}
					File deletedFile = new File("pages/" + last);
					deletedFile.delete();
					if (v.size() > 0) {
						try {
							FileOutputStream fileOut = new FileOutputStream("pages/" + last);
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							out.writeObject(v);
							out.close();
							fileOut.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	// public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
	// String[] strarrOperators)
	// throws DBAppException{
	//
	// }
	public static void main(String[] args) throws DBAppException, IOException {
		String strTableName = "Student";

		Hashtable htblColNameType = new Hashtable();
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
		// createTable(strTableName, "id", htblColNameType);

		Hashtable htblColNameValue = new Hashtable();
		// htblColNameValue.put("id", new Integer(453455));
		// htblColNameValue.put("name", new String("Ahmed Noor"));
		// htblColNameValue.put("gpa", new Double(0.95));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer(5674567));
		// htblColNameValue.put("name", new String("Dalia Noor"));
		// htblColNameValue.put("gpa", new Double(1.25));
		// insertIntoTable(strTableName, htblColNameValue);
		// readTables(strTableName);
		// htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("John Noor"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//
		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("bod da"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//
		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("lolo"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//
		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("sasa"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//
		readTables(strTableName);
		// System.out.println();
		// System.out.println("after push down");
		// System.out.println();

		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("Joe here"));
		// htblColNameValue.put("gpa", null);

		// pushDown(5, strTableName, htblColNameValue);
		// // readTables(strTableName);
		// htblColNameValue.clear();
		// Hashtable hashTable = new Hashtable();
		// hashTable.put("id", new Integer(133));
		// hashTable.put("name", deprecated);
		// hashTable.put("gpa", deprecated);
		// System.out.println(deprecated instanceof String);
		htblColNameValue.put("id", deprecated);
		htblColNameValue.put("name", deprecated);
		htblColNameValue.put("gpa", new Double(1.5));
		deleteFromTable(strTableName, htblColNameValue);
		htblColNameType.clear();
		readTables(strTableName);
		// htblColNameType.clear();
		// System.out.println(search(strTableName, hashTable).toString());
		// hashTable.clear();
		// hashTable.put("id", new Integer(23498));
		// hashTable.put("name", new String("sasa"));
		// hashTable.put("gpa", new Double(1.5));
		// deleteFromTable(strTableName, hashTable);
		// hashTable.clear();
		// readTables(strTableName);

	}

	static class Pair {
		String path;
		int idx;

		Pair(String a, int b) {
			path = a;
			idx = b;
		}

		public String toString() {
			return "Path: " + path + "Index: " + idx + "\n";
		}
	}
}
