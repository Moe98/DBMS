import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

	static int maxTuplesPerPage = 5;
	static ArrayList<Table> tables;
	static String deprecated = "$";
	static String nullEntry = "";

	public static void init() {
		tables = new ArrayList();

		// initialize tables array using metadata
		//
	}

	public static Boolean ExistTable(String strTableName) throws IOException {
		// gives error
		List<List<String>> allinfoInMetaData = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			int co = 0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				// validate types
				if (values[0].equals(strTableName)) {
					return true;
				}
			}

		} catch (Exception ex) {
			return false;
		}
		return false;
	}

	public static Table getTable(String strTableName) {
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).strTableName == strTableName)
				return tables.get(i);
		}
		// method not implemented yet !!!
		return null;
	}

	// try new method
	public static void writeTable() throws IOException {

		try {
			FileOutputStream fileOut = new FileOutputStream("tables1");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tables);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static void readTable() throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream("tables1");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		tables = (ArrayList<Table>) in.readObject();
		in.close();
		fileIn.close();
	}

	public static ArrayList<Pair> search(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		ArrayList<Pair> pageIndex = new ArrayList<>();
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		// for (String colName : htblColNameValue.keySet()) {
		// Object value = htblColNameValue.get(colName);
		// tuple[idx++] = value;
		// }
		tuple = getValueInOrder(strTableName, htblColNameValue);
		for (int jj = 0; jj < paths.length; jj++) {
			String[] x = paths[jj].split("_");
			if (x[0].equals(strTableName)) {
				last = paths[jj];
				counter++;
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size(); i++) {
					o = (Object[]) (v.get(i));
					boolean equals = true;
					for (int j = 0; j < o.length && j < tuple.length; j++) {
						if (!o[j].equals(tuple[j]) && !(tuple[j] instanceof String && tuple[j].equals(deprecated)))
							equals = false;
					}
					if (equals)
						pageIndex.add(new Pair(paths[jj], i));
				}
			}
		}
		return pageIndex;
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		if (!ExistTable(strTableName))
			new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		else
			throw new DBAppException();
		// updates metadata.CSV

	}

	static int getIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		String fileNameDefined = "meta.csv";
		int whichClusCol = 0;
		File meta = new File(fileNameDefined);
		try {
			Scanner inputStream = new Scanner(meta);
			while (inputStream.hasNextLine()) {
				String data = inputStream.nextLine();
				if (data.split(", ")[0].equals(strTableName)) {
					if ((data.split(", ")[3]).equals("False")) {
						whichClusCol++;
					} else {
						whichClusCol++;
						break;
					}
				}
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int index = 0;
		File file = new File("pages/");
		String[] paths = file.list();
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		// int idx = 0;
		// for (String colName : htblColNameValue.keySet()) {
		// Object value = htblColNameValue.get(colName);
		// tuple[idx++] = value;
		// }
		tuple = getValueInOrder(strTableName, htblColNameValue);

		/////////////// sorting the paths///////////////////////////
		paths = sortPaths(paths);
		///////////////////////////////////////////////////
		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {
				last = paths[j];
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size(); i++) {
					o = (Object[]) (v.get(i));
					if (o[whichClusCol] instanceof java.lang.Integer) {
						if (((Integer) o[whichClusCol]).compareTo(((Integer) tuple[whichClusCol])) >= 0) {
							return index;
						}
					} else {
						if (o[whichClusCol] instanceof java.lang.String) {
							if (((String) o[whichClusCol]).compareTo((String) tuple[whichClusCol]) >= 0) {
								return index;
							}
						} else {
							if (o[whichClusCol] instanceof java.lang.Double) {
								if (((Double) o[whichClusCol]).compareTo((Double) tuple[whichClusCol]) >= 0) {
									return index;
								}
							} else {
								if (o[whichClusCol] instanceof Date) {
									if (((Date) o[whichClusCol]).compareTo((Date) tuple[whichClusCol]) >= 0) {
										return index;
									}
								} else {
									if (o[whichClusCol] instanceof java.lang.Boolean) {
										if (((Boolean) o[whichClusCol]).compareTo((Boolean) tuple[whichClusCol]) >= 0) {
											return index;
										}
									}
								}
							}
						}
					}
					index++;
				}
			}
		}
		return index;
	}

	static void validateEntry(String strTableName, Hashtable<String, Object> htblColNameValue, boolean insert)
			throws DBAppException, FileNotFoundException, IOException {
		List<List<String>> allinfoInMetaData = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			int co = 0;
			int f = 0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				// validate types
				if (values[0].equals(strTableName)) {
					co++;
					Object givenValue = htblColNameValue.get(values[1].substring(1));
					if (givenValue == null)
						continue;
					if (values[2].substring(1).equals("java.lang.String"))
						if (!(givenValue instanceof String))
							throw new DBAppException();
					if (values[2].substring(1).equals("java.lang.Integer"))
						if (!(givenValue instanceof Integer))
							throw new DBAppException();
					if (values[2].substring(1).equals("java.lang.double"))
						if (!(givenValue instanceof Double))
							throw new DBAppException();
					if (values[2].substring(1).equals("java.lang.Boolean"))
						if (!(givenValue instanceof Boolean))
							throw new DBAppException();

					if (values[2].substring(1).equals("java.util.Date"))
						if (!(givenValue instanceof Date))
							throw new DBAppException();
					if (givenValue != null && values[3].substring(1).equals("True"))
						f = 1;
				}
			}
			// check entered values are full
			co = insert ? co + 1 : co;
			// if (co != htblColNameValue.size())
			// throw new DBAppException();
			if (f == 0)
				throw new DBAppException();
		}
	}

	public static void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		htblColNameValue.put("TouchDate", new Date(System.currentTimeMillis()));
		validateEntry(strTableName, htblColNameValue, true);

		int p1 = getIndex(strTableName, htblColNameValue);
		// System.out.println(p1);
		pushDown(p1 + 1, strTableName, htblColNameValue);
	}

	static Object[] getValueInOrder(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {

		Object[] tuple;
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			String line;
			int co = 0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				// validate types
				if (values[0].equals(strTableName)) {
					co++;

				}
			}
			tuple = new Object[Math.max(htblColNameValue.size(), co + 1)];
		}
		try (BufferedReader br = new BufferedReader(new FileReader("meta.csv"))) {
			// int co = 0;
			String line;
			tuple[0] = htblColNameValue.get("TouchDate");
			int idx = 1;

			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				// validate types
				if (values[0].equals(strTableName)) {

					Object givenValue = htblColNameValue.get(values[1].substring(1));

					tuple[idx++] = givenValue;
				}
			}
			// System.out.println(Arrays.toString(tuple));
			for (int i = 0; i < tuple.length; i++) {
				if (tuple[i] == null)
					tuple[i] = (Object) "empty cell";
			}

		}
		return tuple;
	}

	static void pushDown(int where, String strTableName, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		where--;
		Object[] tuple = new Object[htblColNameValue.size()];
		int pushed = 0;
		// int idx = 0;
		// for (String colName : htblColNameValue.keySet()) {
		// Object value = htblColNameValue.get(colName);
		// tuple[idx++] = value;
		// }
		tuple = getValueInOrder(strTableName, htblColNameValue);
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String curPath = null;

		int remainingFlag = 0;
		int enteries = 0;

		paths = sortPaths(paths);

		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {
				curPath = paths[j];

				StringTokenizer st = new StringTokenizer(paths[j], "_");
				String num = "";
				while (st.hasMoreTokens())
					num = st.nextToken();
				counter = Math.max(counter, Integer.parseInt(num));

				Vector<Object> newV = new Vector();
				Vector<Object> oldV = getNumberOfTuples("pages/" + curPath);

				if (remainingFlag == 1) {
					newV.add(tuple);
					remainingFlag = 0;
				}

				if (pushed == 0)
					if (enteries + oldV.size() < where) {
						enteries += oldV.size();
						// counter++;
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

				// counter++;
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

	static String[] sortPaths(String[] paths) {
		for (int i = 0; i < paths.length - 1; i++) {
			for (int j = i + 1; j < paths.length; j++) {
				StringTokenizer st = new StringTokenizer(paths[i], "_");
				String crr = "";
				while (st.hasMoreTokens()) {
					crr = st.nextToken();
				}
				int ii = Integer.parseInt(crr);
				st = new StringTokenizer(paths[j], "_");
				while (st.hasMoreTokens()) {
					crr = st.nextToken();
				}
				int jj = Integer.parseInt(crr);
				if (ii >= jj) {
					String temp = paths[i];
					paths[i] = paths[j];
					paths[j] = temp;
				}
			}
		}
		return paths;
	}

	static void readTables(String strTableName) {
		File file = new File("pages/");
		String[] paths = file.list();
		int counter = 0;
		String curPath = null;
		paths = sortPaths(paths);
		// System.out.println(Arrays.toString(paths));
		for (int j = 0; j < paths.length; j++) {
			String[] x = paths[j].split("_");
			if (x[0].equals(strTableName)) {
				curPath = paths[j];
				Vector<Object> curV = getNumberOfTuples("pages/" + curPath);
				System.out.println("pages/" + curPath);
				for (int i = 0; i < curV.size(); i++) {
					Object[] v = (Object[]) curV.get(i);
					for (Object o : v) {
						System.out.print(o.toString() + ", ");
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
			throws DBAppException, FileNotFoundException, IOException {
		validateEntry(strTableName, htblColNameValue, false);
		// System.out.println("below validate");
		File file = new File("pages/");
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size() + 1];
		int idx = 1;
		int idxStrKey = 0;
		// int idxTouchDate = -1;
		// for (String colName : htblColNameValue.keySet()) {
		// Object value = htblColNameValue.get(colName);
		// // if (colName == strKey)
		// // idxStrKey = idx;
		// tuple[idx++] = value;
		// }
		// tuple = getValueInOrder(strTableName,htblColNameValue);
		String col;
		String metaFile = "meta.csv";
		File meta = new File(metaFile);
		boolean found = false;
		try {
			Scanner sc = new Scanner(meta);
			while (sc.hasNext() && !found) {
				String data = sc.nextLine();
				// System.out.println(data);
				String x[] = data.split("_");
				// System.out.println(x[0]);
				if (x[0].startsWith(strTableName)) {
					// System.out.println(data.split(", ")[3]);
					idxStrKey++;
					// System.out.println(idxStrKey);
					if ((data.split(", ")[3]).equals("True")) {
						// col = data.split(", ")[1];
						found = true;
						// System.out.println(idxStrKey + " " + tuple[idxStrKey]);
						break;
					}
				}
			}
			sc.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		// found = false;
		// try {
		// Scanner sc = new Scanner(meta);
		// while (sc.hasNext() && !found) {
		// String data = sc.nextLine();
		// // System.out.println(data);
		// String x[] = data.split("_");
		// // System.out.println(x[0]);
		// if (x[0].startsWith(strTableName)) {
		// // System.out.println(data.split(", ")[3]);
		// idxTouchDate++;
		// // System.out.println(idxStrKey);
		// if ((data.split(", ")[2]).equals("TouchDate")) {
		// // col = data.split(", ")[1];
		// found = true;
		// break;
		// }
		// }
		// }
		// sc.close();
		// } catch (FileNotFoundException e) {
		// e.printStackTrace();
		// }
		// System.out.println(idxStrKey);
		// System.out.println(idxStrKey);
		while (true) {
			boolean tupleFound = false;
			String[] paths = sortPaths(file.list());
			for (int j = 0; j < paths.length; j++) {
				paths = sortPaths(file.list());
				String[] x = paths[j].split("_");
				if (x[0].equals(strTableName)) {
					last = paths[j];
					counter++;
					Vector<Object> v = getNumberOfTuples("pages/" + last);
					Object[] o;
					for (int i = 0; i < v.size();) {
						o = (Object[]) (v.get(i));
						// System.out.println((int) o[2]);
						boolean equals = false;
						// System.out.println(o[idxStrKey].equals(strKey));
						if (o[idxStrKey] instanceof String) {
							equals = ((String) (o[idxStrKey])).equals(strKey);
						}
						if (o[idxStrKey] instanceof Double) {
							equals = ((Double) o[idxStrKey]) == Double.parseDouble(strKey);
							// type = "double";
						}
						if (o[idxStrKey] instanceof Integer) {
							equals = ((Integer) o[idxStrKey]) == Integer.parseInt(strKey);
							// System.out.println("in here" + " " + equals);
							// type = "integer";
						}
						if (o[idxStrKey] instanceof Boolean) {
							equals = ((Boolean) o[idxStrKey]) == Boolean.parseBoolean(strKey);
							// type = "boolean";
						}
						if (o[idxStrKey] instanceof Date) {
							equals = ((Date) o[idxStrKey]).equals(new Date(strKey));
							// type = "date";
						}
						// if (o[idxStrKey].equals((Object) strKey))
						// equals = true;
						if (equals) {
							// System.out.println(idxTouchDate);
							// System.out.println(tuple[idxTouchDate]);
							// System.out.println("equals");

							// tuple[0] = new Date(System.currentTimeMillis());
							htblColNameValue.put("TouchDate", new Date(System.currentTimeMillis()));
							tuple = getValueInOrder(strTableName, htblColNameValue);
							// System.out.println("in delete");
							v.remove(i);
							// i = 0;
							j = 0;
							i = 0;
							tupleFound = true;
							// if(v.size()==0)
							// j=j==0?j:j-1;
						} else
							i++;
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
						if (equals) {
							// System.out.println("in insert");
							insertIntoTable(strTableName, htblColNameValue);
							// v.add(tuple);
							// readTables(strTableName);
						}
						if (i == 0 && j == 0 && equals)
							break;
					}
				}
			}
			if (!tupleFound)
				break;
		}
	}

	public static void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, FileNotFoundException, IOException {
		File file = new File("pages/");
		String[] paths = sortPaths(file.list());
		int counter = 0;
		String last = null;
		Object[] tuple = new Object[htblColNameValue.size()];
		int idx = 0;
		// for (String colName : htblColNameValue.keySet()) {
		// Object value = htblColNameValue.get(colName);
		// tuple[idx++] = value;
		// }
		htblColNameValue.put("TouchDate", new Date(System.currentTimeMillis()));
		tuple = getValueInOrder(strTableName, htblColNameValue);
		for (int jj = 0; jj < paths.length; jj++) {
			String[] x = paths[jj].split("_");
			if (x[0].equals(strTableName)) {
				last = paths[jj];
				counter++;
				Vector<Object> v = getNumberOfTuples("pages/" + last);
				Object[] o;
				for (int i = 0; i < v.size();) {
					o = (Object[]) (v.get(i));
					boolean equals = true;
					// System.out.println("Inside tuple");
					// for (int j = 0; j < tuple.length; j++)
					// System.out.println("Index of: " + j + " Value: " + tuple[j].toString());
					// System.out.println("Inside o");
					// for (int j = 0; j < o.length; j++)
					// System.out.println("Index of: " + j + " Value: " + o[j].toString());
					// System.out.println(o.length + " " + tuple.length);
					for (int j = 1; j < o.length; j++)
						if (!(o[j] == null) && !o[j].equals(tuple[j])
								&& !(tuple[j] instanceof String && tuple[j].equals(deprecated)))
							equals = false;
					if (equals)
						v.remove(i);
					else
						i++;
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

	public static void main(String[] args) throws DBAppException, IOException, ParseException {
		String strTableName = "teamDB";
		String string = "January 2, 2009";

		Hashtable htblColNameType = new Hashtable();
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
		// htblColNameType.put("date", "java.util.Date");
		// createTable(strTableName, "id", htblColNameType);

		Hashtable htblColNameValue = new Hashtable();
		// htblColNameValue.put("id", new Integer(2));
		// // htblColNameValue.put("name", new String("z"));
		// htblColNameValue.put("gpa", new Double(0.95));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();

		////////////////////////////////////////////////////

		//////////////////////////////////////////
		// string = "January 2, 2007";
		// htblColNameValue.put("id", new Integer(1));
		// // htblColNameValue.put("name", new String("d"));
		// htblColNameValue.put("gpa", new Double(1.25));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//////////////////////////////////////////////////////
		// string = "January 2, 2006";
		// // htblColNameValue.put("name", new String("c"));
		// htblColNameValue.put("date", new Date(string));
		// htblColNameValue.put("id", new Integer(11));
		// // htblColNameValue.put("name", new String("c"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// readTables(strTableName);
		////////////////////////////////////////////////
		// string = "January 2, 2006";
		// htblColNameValue.put("id", new Integer(7));
		// // htblColNameValue.put("name", new String("c"));
		// // htblColNameValue.put("gpa", new Double(1.5));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		///////////////////////////////////////////////////////

		// string = "January 2, 2017";
		// htblColNameValue.put("id", new Integer(5));
		// htblColNameValue.put("name", new String("a"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();

		//////////////////////////////////////////////////////
		// string = "January 2, 2018";
		// htblColNameValue.put("id", new Integer(6));
		// htblColNameValue.put("name", new String("d"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		//////////////////////////////////////////////////////
		// string = "January 2, 1950";
		// htblColNameValue.put("name", new String("y"));
		// htblColNameValue.put("gpa", new Double(1.3));
		// htblColNameValue.put("id", new Integer(11));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();

		//////////////////////////////////////////////////////
		// string = "December 15, 1998";
		// htblColNameValue.put("id", new Integer(15));
		// htblColNameValue.put("name", new String("Moe"));
		// htblColNameValue.put("gpa", new Double(2.7));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// readTables(strTableName);

		////////////////////////////////////////////////////
		// string = "January 2, 1999";
		// htblColNameValue.put("id", new Integer(2));
		// htblColNameValue.put("name", new String("abcdefg"));
		// htblColNameValue.put("gpa", new Double(4.4255));
		// htblColNameValue.put("date", new Date(string));
		// insertIntoTable(strTableName, htblColNameValue);
		// readTables(strTableName);

		///////////////////////////////////////////////////
		string = "January 2, 1999";
		htblColNameValue.put("id", new Integer(3));
		// htblColNameValue.put("name", new String("abcdefg"));
		htblColNameValue.put("gpa", new Double(4.4255));
		htblColNameValue.put("date", new Date(string));
		updateTable(strTableName, "6", htblColNameValue);
		htblColNameValue.clear();
		readTables(strTableName);
		// System.out.println(new Date(System.currentTimeMillis()));
		///////////////////////////////////////////////////
		// htblColNameValue.put("id", new Integer(11));
		// htblColNameValue.put("name", deprecated);
		// htblColNameValue.put("gpa", deprecated);
		// htblColNameValue.put("date", deprecated);
		// deleteFromTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
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
