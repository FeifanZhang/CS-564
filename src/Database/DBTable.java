package Database;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.LinkedList;

public class DBTable {
    private RandomAccessFile rows;//the file that stores the rows in the table
    private long free;//head of the free list space for rows
    private int numOtherFields;
    private int otherFieldLengths[];
    private ExtHash extHash;

    //constructor method create new table
    public DBTable(String filename, int fL[], int bsize ) {
        //create the new DBTable
        try{
            for (int x:fL){
                if(x<=0){
                    throw new ExceptionInInitializerError("constructor method,the element should >0");
                }
            }File file=new File("src/TextFile/"+filename);
            if(file.exists()){
                file.delete();
            }this.otherFieldLengths=fL;
            this.numOtherFields=fL.length;
            this.rows = new RandomAccessFile(file,"rw");
            String tuple;
            for(int i=-1;i<=this.numOtherFields;i++){
                if(i==-1){
                    tuple=Long.toString(this.rows.getFilePointer())+';'+this.numOtherFields+'\n';
                }else if(i>=0 && i<this.numOtherFields){
                    tuple=Long.toString(this.rows.getFilePointer())+';'+this.otherFieldLengths[i]+'\n';
                }else{
                    this.free=this.rows.getFilePointer();
                    int counter=0;
                    for (int a:this.otherFieldLengths) {
                        counter+=a;
                    }counter+=this.otherFieldLengths.length+6;
                    tuple=Long.toString(this.rows.getFilePointer())+';'+String.format("% "+counter+"d",0)+'\n';
                }this.rows.writeBytes(tuple);
            }this.extHash=new ExtHash(filename,bsize);/*creating the ExtHash would use bsize to init it*/
        }catch (IOException e ){
            e.printStackTrace();
        }catch (ExceptionInInitializerError e){
            e.printStackTrace();
        }catch (NullPointerException e){
            e.printStackTrace();
        }
    }

    //constructor method open table
    public DBTable(String filename) {
        try{
            this.rows=new RandomAccessFile("src/TextFile/"+filename,"rw");
            this.extHash=new ExtHash(filename);
            String tuple;
            tuple=this.rows.readLine();
            this.numOtherFields=Integer.parseInt(tuple.split(";")[1]);
            this.otherFieldLengths=new int[this.numOtherFields];
            for(int i=0;i<=this.numOtherFields;i++){
                tuple=this.rows.readLine();
                if(i<this.numOtherFields){
                    this.otherFieldLengths[i]=Integer.parseInt(tuple.split(";")[1]);
                }else{
                    this.free=Long.valueOf(tuple.split(";")[0]);
                }
            }this.extHash=new ExtHash(filename);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public boolean remove(int key) {
        long addr=this.extHash.remove(key);
        if(addr==0){//key is not exist
            return false;
        }else {
            try{
                Long endofFree=getFree(this.free,Long.valueOf("0"));
                Row tuple=new Row(addr);
                tuple.becomeFreeIndex(0);
                this.rows.seek(addr);
                //writing as a byte, should transfer the long into string.
                this.rows.writeBytes(String.valueOf(addr)+';'+tuple.toString()+'\n');
                Row tuple1=new Row(endofFree);
                tuple1.becomeFreeIndex(Integer.parseInt(String.valueOf(addr)));
                this.rows.seek(endofFree);
                this.rows.writeBytes(String.valueOf(endofFree)+';'+tuple1.toString()+'\n');
            }catch(IOException e){
                e.printStackTrace();
                return false;
            }
        }return true;
    }

    public boolean insert(int key, char fields[][]) {
        /* in false, using exthash table to search if the key exist*/
        if(this.extHash.search(key)!=0){
            return false;
        }else if(Integer.toString(key).length()>=6){
            return false;
        }else {
            Long addr=getInsertAddr();
            try{
                this.rows.seek(addr);
                this.extHash.insert(key,addr);
                this.rows.writeBytes(addr+";"+new Row(key,fields).toString()+'\n');
                addr=getFree(this.free,Long.valueOf(addr));
                Row tuple1=new Row(addr);
                tuple1.becomeFreeIndex(0);
                this.rows.seek(addr);
                this.rows.writeBytes(this.rows.getFilePointer()+";"+tuple1.toString()+'\n');
                return true;
            }catch (IOException e){
                e.printStackTrace();
                return false;
            }
        }
    }

    private Long getFree(Long addr,Long dest){
        try{
            this.rows.seek(addr);
            String []tuple=this.rows.readLine().split(";");
            Long pointTo=Long.parseLong(tuple[1].replace(" ",""));
            if(pointTo.equals(dest)){
                return addr;
            }else {
                return this.getFree(pointTo,dest);
            }
        }catch (IOException e){
            e.printStackTrace();
        }return addr;
    }

    private Long getInsertAddr(){
        Long addr=getFree(this.free,Long.valueOf("0"));
        try{
            if(addr==this.free){
                this.rows.seek(addr);
                int counter=0;
                for (int a:this.otherFieldLengths) {
                    counter+=a;
                }counter+=this.otherFieldLengths.length+6;
                this.rows.writeBytes(Long.toString(this.rows.getFilePointer())+';'+String.format("% "+counter+"d",this.rows.length())+'\n');
                Long result=this.rows.length();
                this.rows.seek(this.rows.length());
                this.rows.writeBytes(Long.toString(this.rows.getFilePointer())+';'+String.format("% "+counter+"d",0)+'\n');
                return result;
            }else {
                return addr;
            }
        }catch (IOException e){
            e.printStackTrace();
        }return addr;
    }

    public LinkedList<String> search(int key) {
        long addr=this.extHash.search(key);
        LinkedList<String> result=new LinkedList<>();
        if(addr==Long.valueOf("0")){
            result.add("key is not exist");
        }else {
            Row row=new Row(addr);
            for (char[] a:row.otherFields) {
                result.add(String.valueOf(a).replace("\u0000",""));
            }
        }return result;
    }

    public void close() {
        if(this.rows!=null){
            try{
                this.rows.close();
            }catch (IOException e){
                e.printStackTrace();
            }this.extHash.close();
        }
    }

    private class Row {
        private int keyField;
        private char otherFields[][];
        private boolean isIndex;

        private Row(int key,char oF[][]){
            this.keyField=key;
            this.otherFields=new char [numOtherFields][];
            try{
                if(oF.length!=this.otherFields.length){
                    throw new IndexOutOfBoundsException("public Row,data.length!=numOtherFields");
                }for(int x=0;x<this.otherFields.length;x++){
                    if(otherFieldLengths[x]>=oF[x].length){
                        this.otherFields[x]=new char[otherFieldLengths[x]];
                        for(int y=0;y<otherFieldLengths[x];y++){
                            try{
                                this.otherFields[x][y]=oF[x][y];
                            }catch (ArrayIndexOutOfBoundsException e){
                                this.otherFields[x][y]=' ';
                            }
                        }
                    }else {
                        System.out.print(Integer.toString(otherFieldLengths[x])+'\n');
                        throw new IndexOutOfBoundsException("public Row,data.otherFieldLengths[x]<this.otherFields[x].length");
                    }
                }
            }catch (NullPointerException e){
                e.printStackTrace();
            }this.isIndex=false;
        }

        private Row(Long addr){
            this.otherFields=new char [numOtherFields][];
            try{
                rows.seek(addr);
                String tuple=rows.readLine();
                String[] data=tuple.split(";");
                if(data.length==2){//free index or end of free
                    this.keyField=Integer.parseInt(data[1].replace(" ",""));
                    for(int i=0;i<this.otherFields.length;i++){
                        this.otherFields[i]=new char[otherFieldLengths[i]];
                        for(int x=0;x<this.otherFields[i].length;x++){
                            this.otherFields[i][x]=' ';
                        }
                    }this.isIndex=true;
                }else {// not the end of free
                    if(data.length-2!=this.otherFields.length){
                        throw new IndexOutOfBoundsException("public Row,data.length!=numOtherFields");
                    }for(int i=0;i<data.length;i++){
                        if(i==1){
                            this.keyField=Integer.parseInt(data[i].replace(" ",""));
                        }else if(i>1){
                            this.otherFields[i-2]=new char[data[i].length()];
                            for(int x=0;x<data[i].length();x++) {
                                this.otherFields[i-2][x]=data[i].charAt(x);
                            }
                        }
                    }this.isIndex=false;
                }
            }catch (IOException e){
                e.printStackTrace();
            }catch (NullPointerException e){
                e.printStackTrace();
            }
        }

        private void becomeFreeIndex(int addr){
            this.keyField=addr;
            for(int i=0;i<this.otherFields.length;i++){
                for(int x=0;x<this.otherFields[i].length;x++){
                    this.otherFields[i][x]=' ';
                }
            }this.isIndex=true;
        }

        public String toString(){
            String tuple;
            if(this.isIndex){//the index or free
                int counter=0;
                for (char[] a:this.otherFields) {
                    counter+=a.length;
                }counter+=this.otherFields.length+6;
                tuple=String.format("% "+counter+"d",this.keyField);
            }else{//the tuple
                tuple=String.format("% 6d",this.keyField);
                for (char[] str:this.otherFields) {
                    tuple = tuple+';';
                    for (char x:str) {
                        tuple = tuple+x;
                    }
                }
            }return tuple;
        }
    }

    public String toString(){
        String table="";
        try{
            Long position=this.rows.getFilePointer();
            this.rows.seek(0);
            while (this.rows.getFilePointer()<this.rows.length()){
                table+=this.rows.readLine()+'\n';
            }this.rows.seek(position);
        }catch (IOException e){
            e.printStackTrace();
        }return table;
    }
}
