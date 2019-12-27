package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;
import android.net.Uri;
import android.database.Cursor;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        int i = 0;
//        while (i < 25) {

            String data = "@";
            String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
            Uri providerURI = Uri.parse("content://" + authority);
//            try{
//                Cursor c = getContentResolver().query(providerURI, null, data, null, null);
//                c.moveToFirst();
////            System.out.println("key ----->" + c.getString(0));
////            System.out.println("value ----->" + c.getString(1));
//
//                int size = c.getCount();
//                System.out.println("size - " + size + "\n");
//                for(i=0; i < size; i++) {
//                    System.out.println("key ----->" + c.getString(0));
//                    System.out.println("value ----->" + c.getString(1) + "\n");
//                    tv.append("\t" + c.getString(0) + " " + c.getString(1) + "\n\n");
//                    c.moveToNext();
//                }
//            } catch(Exception e){
//
//            }
//            i++;
//        }

	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
