package RDBMS;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class Test {
		
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		Hashtable htblColNameValue = new Hashtable( );
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("date", "java.lang.Date");
//		DBApp.createTable( strTableName, "id", htblColNameType );
//		
		htblColNameValue.put("id", new Integer( 2000));
//		htblColNameValue.put("name", new String("Zoz" ) );
//		htblColNameValue.put("gpa", new Double( 0.95 ) );
//		htblColNameValue.put("date", new Date(1997-1900,12-1,14));
		DBApp.deleteFromTable(strTableName, htblColNameValue);
//		DBApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 1200 ));
//		htblColNameValue.put("name", new String("Joe" ) );
//		htblColNameValue.put("gpa", new Double( 3 ) );
//		DBApp.updateUsingBitmap(strTableName, "2000",htblColNameValue);
//		DBApp.insertIntoTable(strTableName , htblColNameValue );
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 1400 ));
//		htblColNameValue.put("name", new String("Dalia Noor" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		DBApp.updateUsingBitmap(strTableName, "2000",htblColNameValue);
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 200 ));
//		htblColNameValue.put("name", new String("John Noor" ) );
//		htblColNameValue.put("gpa", new Double( 1.5 ) );
//		DBApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 78452 ));
//		htblColNameValue.put("name", new String("Zaky Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.88 ) );
//		DBApp.insertIntoTable( strTableName , htblColNameValue );
		DBApp.readTables("Student");
//		DBApp.createBitmapIndex(strTableName, "id");
		Bitmap.readBitmap(strTableName, "id");
	}

}
