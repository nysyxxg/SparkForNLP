package Utils;

import Server2.LoadModel;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by MingDong on 2016/9/30.
 */
public class ListToString {
    public static void main(String[] args) throws Exception {
        String str = "aaa@@bbb@@ccc";
        List<String> list = new ArrayList<String>();
       /* String strs[] = str.split("@@");
        for (String s : strs) {
           String result = 123+s;
            list.add(result);
        }*/

        list.add("aaa");
        list.add("bbb");
        list.add("ccc");
        StringBuilder result = new StringBuilder();
        boolean flag = false;
        for (String string : list) {
            if (flag) {
                result.append("@@");
            } else {
                flag = true;
            }
            result.append(string);
        }
        System.out.println(result.toString());//aaa,bbb,ccc
        //System.out.println(list);
    }
}
