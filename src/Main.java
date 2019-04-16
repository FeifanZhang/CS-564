import Database.DBTable;
import Database.*;
import java.io.IOException;

public class Main {

    public static void main(String[] args) {
//        try{
//            h2a();
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//        overFlow();
    }

    public static void h2a()throws IOException {
        new h2a();
    }

    public static void h2b()throws IOException {
        new h2b();
    }

    public static void overFlow(){
        int[] a =new int[4];
        for(int i =0;i<a.length;i++){
            a[i]=i+1; }
        DBTable db=new DBTable("hello",a,2);
        //DBTable db=new DBTable("hello");

        for(int i=0;i<=4;i++){
            int insert=i*4096+1;
            System.out.print("the key for insert is: "+insert+'\n');
            char [][]tuple={{String.valueOf(i).charAt(0)},{'a','b'},{'a','b','c'},{'a','b','c','d'}};
            db.insert(insert,tuple);
        }System.out.print("-----------------------------------------------"+'\n');
        for(int i=0;i<=4;i++){
            int c=i*4096+1;
            System.out.print("the key is"+c+db.search(i*4096+1)+'\n');
        }System.out.print("-----------------------------------------------"+'\n');
        for(int i=0;i<=4;i+=2){
            int remove=i*4096+1;
            System.out.print("remove the"+remove+'\n');
            db.remove(remove);
        }System.out.print("-----------------------------------------------"+'\n');
        for(int i=0;i<=4;i++){
            int d=i*4096+1;
            System.out.print("the key is"+d+db.search(d)+'\n');
        }System.out.print("remove 4097"+'\n');
        db.remove(4097);
        System.out.print("-----------------------------------------------"+'\n');
        for(int i=0;i<=4;i++){
            int e=i*4096+1;
            System.out.print("the key is"+e+db.search(e)+'\n');
        }

    }

}
