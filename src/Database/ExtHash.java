package Database;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.File;
import java.util.*;

public class ExtHash{

    private RandomAccessFile buckets;
    private RandomAccessFile directory;
    private RandomAccessFile extendBucket;
    private int bucketSize;
    private int directoryBIts;
    private long bucketFree;
    private int maxLengthOfHash=12;

    public ExtHash(String filename, int bsize) {
        try{
            if(bsize<=0){
                throw new ExceptionInInitializerError("bsize should > 0");
            }File bucket = new File("src/TextFile/"+filename+"buckets");
            File dir=new File("src/TextFile/"+filename+"dir");
            File extBucket=new File("src/TextFile/"+filename+"extBucket");
            if(bucket.exists() && dir.exists() && extBucket.exists()){
                bucket.delete();
                dir.delete();
                extBucket.delete();
            }this.bucketSize=bsize;
            this.directoryBIts=1;
            this.buckets=new RandomAccessFile(bucket,"rw");
            this.directory=new RandomAccessFile(dir,"rw");
            this.extendBucket=new RandomAccessFile(extBucket,"rw");
            this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+this.bucketSize+'\n');
            this.directory.writeBytes(String.valueOf(this.directory.getFilePointer())+';'+String.format("% 6d",directoryBIts)+'\n');
            String array="";
            int counter=0;
            while(counter<this.bucketSize){
                if(counter==this.bucketSize-1){
                    array+=String.format("% 6d",0);
                }else {
                    array+=String.format("% 6d",0)+',';
                }counter++;
            }this.bucketFree=this.buckets.getFilePointer();
            int numOfSpace=bsize*14+13;
            this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+String.format("% "+numOfSpace+"d",0)+'\n');
            this.directory.writeBytes(String.valueOf(this.directory.getFilePointer())+';'+String.format("% 6d",this.buckets.getFilePointer())+'\n');
            String bucketTuple=String.valueOf(this.buckets.getFilePointer())+';'+String.format("% 6d",1)+';'+String.format("% 6d",0)+';'+array+';'+array+'\n';
            this.buckets.writeBytes(bucketTuple);
            this.directory.writeBytes(String.valueOf(this.directory.getFilePointer())+';'+String.format("% 6d",this.buckets.getFilePointer())+'\n');
            bucketTuple=String.valueOf(this.buckets.getFilePointer())+';'+String.format("% 6d",1)+';'+String.format("% 6d",0)+';'+array+';'+array+'\n';
            this.buckets.writeBytes(bucketTuple);
        }catch (IOException e){
            e.printStackTrace();
        }catch (ExceptionInInitializerError e){
            e.printStackTrace();
        }
    }

    public ExtHash(String filename) {
        //open an existing hash index
        //the associated directory file is named filename+”dir” //the associated bucket file is named filename+”buckets” //both files should already exists when this method is used
        try{
            this.buckets=new RandomAccessFile("src/TextFile/"+filename+"buckets","rw");
            this.bucketSize=Integer.valueOf(this.buckets.readLine().split(";")[1]);
            this.bucketFree=Long.valueOf(this.buckets.readLine().split(";")[0].replace(" ",""));
            this.directory=new RandomAccessFile("src/TextFile/"+filename+"dir","rw");
            this.directoryBIts=Integer.valueOf(this.directory.readLine().split(";")[1].replace(" ",""));
            this.extendBucket=new RandomAccessFile("src/TextFile/"+filename+"extBucket","rw");
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    private class Bucket{
        private int bucketBits; //the number of hash function bits used by this bucket
        private int count; // the number of keys are in the bucket
        private int keys[];
        private long rowAddrs[];
        private boolean isIndex;

        private Bucket(int nBit,int bsize){
            this.bucketBits=nBit;
            this.count=0;
            this.keys=new int[bsize];
            this.rowAddrs=new long[bsize];
            this.isIndex=false;
        }

        private Bucket(Long addr){
            try{
                buckets.seek(addr);
                String line=buckets.readLine();
                if(addr==buckets.length() || line.split(";").length==2 ){//final line or index
                    this.bucketBits=1;
                    this.keys=new int[bucketSize];
                    this.rowAddrs=new long[bucketSize];
                    this.count=Integer.valueOf(line.split(";")[1].replace(" ",""));
                    for(int i=0;i<this.keys.length;i++){
                        this.keys[i]=0;
                        this.rowAddrs[i]=Long.valueOf("0");
                    }this.isIndex=true;
                }else {//normal data
                    String[] tuple=line.split(";");
                    for(int i=0;i<tuple.length;i++){
                        if(i==1){//nBit
                            this.bucketBits=Integer.valueOf(tuple[i].replace(" ",""));
                        }else if(i==2){//nKeys
                            this.count=Integer.valueOf(tuple[i].replace(" ",""));
                        }else if(i==3){//keys
                            String []tupleKeys=tuple[i].split(",");
                            this.keys=new int[tupleKeys.length];
                            for (int x=0;x<tupleKeys.length;x++) {
                                this.keys[x]=Integer.valueOf(tupleKeys[x].replace(" ",""));
                            }
                        }else if(i==4){//table addrs
                            this.rowAddrs=new long [tuple[i].split(",").length];
                            String[] tupleAddrs=tuple[i].split(",");
                            for(int x=0;x<this.rowAddrs.length;x++){
                                this.rowAddrs[x]=Long.valueOf(tupleAddrs[x].replace(" ",""));
                            }
                        }
                    }this.isIndex=false;
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        private Long searchKey(int key) {
            for (int i=0;i<this.keys.length;i++) {
                if(this.keys[i]!=0 && this.keys[i]!=key && ifHashEqual(this.keys[i],key)){//key might in extend bucket
                    return new ExtBucket(this.rowAddrs[i]).search(key);
                }
            }for(int i=0;i<this.keys.length;i++){
                if(key==this.keys[i]){
                    return this.rowAddrs[i];
                }
            }return Long.valueOf("0");
        }

        private boolean insert(int key,long addr) {
            for (int i=0;i<this.keys.length;i++) {
                if(this.keys[i]!=0 && this.keys[i]!=key && ifHashEqual(this.keys[i],key)){//if hash overflow
                    return new ExtBucket(this.rowAddrs[i]).insert(key,addr);
                }
            }if(this.count>=this.keys.length){
                return false;
            }else {
                for(int i=0;i<this.keys.length;i++){
                    if(this.keys[i]==0){
                        this.keys[i]=key;
                        this.rowAddrs[i]=addr;
                        this.count++;
                        this.isIndex=false;
                        break;
                    }
                }return true;
            }
        }

        private long remove(int key){
            for (int i=0;i<this.keys.length;i++) {
                if(this.keys[i]!=0 && this.keys[i]!=key && ifHashEqual(this.keys[i],key)){//key might in extend bucket
                    return new ExtBucket(this.rowAddrs[i]).remove(key);
                }
            }if(this.searchKey(key).equals(Long.valueOf("0"))){//key not exist
                return Long.valueOf("0");
            }else {//key exist
                for(int i=0;i<this.keys.length;i++){
                    if(this.keys[i]==key){
                        String[] overFlow=new ExtBucket(this.rowAddrs[i]).popSameHashKey().replace(" ","").split(";");
                        this.keys[i]=Integer.valueOf(overFlow[0]);
                        long result=this.rowAddrs[i];
                        this.rowAddrs[i]=Long.valueOf(overFlow[1]);
                        if(this.keys[i]==0){// no exist key that have same hash value
                            this.count--;
                        }return result;
                    }
                }
            }return Long.valueOf("0");
        }

        public String toString(){
            String row;
            if(this.isIndex){
                int numOfSpace=bucketSize*14+13;
                row=String.format("% "+numOfSpace+"d",this.count);
            }else {
                row=String.format("% 6d",this.bucketBits)+';'+String.format("% 6d",this.count)+';';
                for(int i=0;i<this.keys.length;i++){
                    if(i==this.keys.length-1){
                        row+=String.format("% 6d",this.keys[i]);
                    }else {
                        row+=String.format("% 6d",this.keys[i])+',';
                    }
                }row+=';';
                for(int i=0;i<this.rowAddrs.length;i++){
                    if(i==this.rowAddrs.length-1){
                        row+=String.format("% 6d",this.rowAddrs[i]);
                    }else {
                        row+=String.format("% 6d",this.rowAddrs[i])+',';
                    }
                }
            }return row;
        }
    }

    private class ExtBucket{
        private ArrayList<Long> addr;
        private ArrayList<Long> id;
        private ArrayList<Long> addrForKey;
        private ArrayList<Integer> key;
        //addr;id for 6 space;addr for key for 6 space;key for 6 space
        private ExtBucket(long id){
            try{
                this.addr=new ArrayList<>();
                this.id=new ArrayList<>();
                this.addrForKey=new ArrayList<>();
                this.key=new ArrayList<>();
                extendBucket.seek(Long.valueOf("0"));
                while(extendBucket.getFilePointer()<extendBucket.length()){
                    String[]tuple=extendBucket.readLine().replace(" ","").split(";");
                    if(id==Long.valueOf(tuple[1])){
                        this.addr.add(Long.valueOf(tuple[0]));
                        this.id.add(Long.valueOf(tuple[1]));
                        this.addrForKey.add(Long.valueOf(tuple[2]));
                        this.key.add(Integer.valueOf(tuple[3]));
                    }
                }if(key.isEmpty()){//no key have same value with the value in bucket
                    this.id.add(id);
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        private String popSameHashKey(){//return: key;addr
            String result;
            if(this.key.isEmpty()){
                result="0;0";
            }else {
                for(int a=0;a<this.key.size();a++){
                    if (!this.key.get(a).equals(0)) {
                        result=String.valueOf(this.key.get(a))+';'+this.addrForKey.get(a);
                        for (int i=0;i<this.id.size();i++){
                            this.id.set(i,this.addrForKey.get(a));
                        }this.key.set(a,0);
                        this.id.set(a,Long.valueOf("0"));
                        this.addrForKey.set(a,Long.valueOf("0"));
                        this.storeIntoFile();
                        return result;
                    }
                }result="0;0";

            }return result;
        }

        private long search(int key){
            Long result=Long.valueOf("0");
            if(!this.key.isEmpty()){
                for(int i=0;i<this.key.size();i++){
                    if(this.key.get(i).equals(key)){
                        result= this.addrForKey.get(i);
                    }
                }
            }return result;
        }

        private long remove(int key){
            long result=Long.valueOf("0");
            if(!this.key.isEmpty()){
                for(int i=0;i<this.key.size();i++){
                    if(this.key.get(i).equals(key)){
                        result=this.addrForKey.get(i);
                        this.key.set(i,0);
                        this.addrForKey.set(i,Long.valueOf("0"));
                        this.id.set(i,Long.valueOf("0"));
                        break;
                    }
                }this.storeIntoFile();
            }return result;
        }

        private boolean insert(int key,long addr){
            if(this.search(key)==0){//key not exist
                this.addr.add(this.getInsertAddr());
                if(!this.id.isEmpty()){// this is the first key that have same value in bucket
                    this.id.add(id.get(0));
                }this.key.add(key);
                this.addrForKey.add(addr);
                this.storeIntoFile();
                return true;
            }return false;
        }

        private void storeIntoFile(){
            if(!this.addr.isEmpty()){
                try{
                    for(int i=0;i<this.key.size();i++){
                        extendBucket.seek(addr.get(i));
                        extendBucket.writeBytes(String.valueOf(addr.get(i))+';'+String.format("% 6d",id.get(i))+';'
                                +String.format("% 6d",this.addrForKey.get(i))+';'+String.format("% 6d",this.key.get(i))+'\n');
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }

        private Long getInsertAddr(){
            try{
                extendBucket.seek(0);
                while(extendBucket.getFilePointer()<extendBucket.length()){
                    long addr=extendBucket.getFilePointer();
                    String[] tuple=extendBucket.readLine().replace(" ","").split(";");
                    if(tuple[1].equals("0")){
                        return addr;
                    }
                }return extendBucket.length();
            }catch (IOException e){
                e.printStackTrace();
            }return Long.valueOf("-1");
        }
    }

    public boolean insert(int key,long addr) {
        try {
            this.directory.seek(this.getDir(key));
            long oldBucketAddr=Long.valueOf(this.directory.readLine().split(";")[1].replace(" ",""));
            Bucket oldBucket=new Bucket(oldBucketAddr);
            if(oldBucket.searchKey(key).equals(Long.valueOf("0"))){//key can be insert
                if(!oldBucket.insert(key,addr)){//bucket full
                    oldBucket.bucketBits++;
                    long newBucketAddr=getInsertAddr();
                    Bucket newBucket=new Bucket(newBucketAddr);
                    newBucket.count=0;
                    newBucket.bucketBits=oldBucket.bucketBits;
                    if(this.directoryBIts<oldBucket.bucketBits){//need to extend the dir
                        this.directory.seek(0);
                        this.directory.readLine();
                        long oldLength=this.directory.length();
                        long copyRowAddr=this.directory.getFilePointer();
                        while(copyRowAddr<oldLength){
                            String insertRow=String.valueOf(this.directory.length())+';'+this.directory.readLine().split(";")[1]+'\n';
                            copyRowAddr=this.directory.getFilePointer();// store the address of the next row in  dir
                            this.directory.seek(this.directory.length());
                            this.directory.writeBytes(insertRow);
                            this.directory.seek(copyRowAddr);
                        }this.directoryBIts++;
                        this.directory.seek(0);
                        this.directory.writeBytes(String.valueOf(this.directory.getFilePointer())+';'+String.format("% 6d",this.directoryBIts)+'\n');
                    }for(int i=0;i<oldBucket.keys.length;i++){// 遍历一遍oldBucket中的key,确定哪些要放在newBucket里面
                        String hashCode=this.hash(oldBucket.keys[i]);
                        if(hashCode.charAt(hashCode.length()-newBucket.bucketBits)=='1'){//key 转移至newBucket的条件
                            newBucket.insert(oldBucket.keys[i],oldBucket.rowAddrs[i]);
                            oldBucket.remove(oldBucket.keys[i]);
                        }
                    }String hashCode=this.hash(key);
                    if(hashCode.charAt(hashCode.length()-newBucket.bucketBits)=='1'){//将新插入的key addr放入规定的bucket中
                        newBucket.insert(key, addr);
                    }else {
                        oldBucket.insert(key, addr);
                    }this.buckets.seek(oldBucketAddr);// write the new & old bucket into bucket file.
                    this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+oldBucket.toString()+'\n');
                    this.buckets.seek(newBucketAddr);
                    newBucket.isIndex=false;
                    this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+newBucket.toString()+'\n');
                    this.buckets.seek(getBucketFree(this.bucketFree,newBucketAddr));// 将倒数第二个free变为最后一个
                    int counter=this.bucketSize*14+13;
                    this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+String.format("% "+counter+"d",0));
                    Map map=this.pointSameBucket(oldBucketAddr);
                    int compare=oldBucket.bucketBits;
                    Iterator <Map.Entry<Integer, String[]>> iterator = map.entrySet().iterator();
                    while (iterator.hasNext()){//遍历所有指向oldBucketAddr的dir让其中规定位==1的指向newBucketAddr
                        Map.Entry<Integer,String[]> entry=iterator.next();
                        String hash=this.hash(entry.getKey());
                        if(hash.charAt(hash.length()-compare)=='1'){
                            this.directory.seek(Long.valueOf(entry.getValue()[0]));
                            String tuple=entry.getValue()[0]+';'+String.format("% 6d",newBucketAddr);
                            this.directory.writeBytes(tuple);
                        }
                    }
                }else {//key has been insert in bucket
                    this.buckets.seek(oldBucketAddr);
                    this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+oldBucket.toString()+'\n');
                }if(this.search(key)==0){//像是(1000,100,10000)这类数字
                   return this.insert(key,addr);
                }else {
                    return true;
                }
            }else {
                return false;
            }
        }catch (IOException e){
            e.printStackTrace();
        }return false;
    }

    public long remove(int key) {
        long deleteDBAddr=this.search(key);
        if(deleteDBAddr!=0){//the key is exist
            try{
                this.directory.seek(getDir(key));
                long bucketAddr=Long.valueOf(this.directory.readLine().split(";")[1].replace(" ",""));
                Bucket bucket=new Bucket(bucketAddr);
                long result=bucket.remove(key);
                if(bucket.count==0){//bucket has been empty
                    int compareBit=--bucket.bucketBits;
                    Map<Integer,String[]> pointToEmptyBucket=this.pointSameBucket(bucketAddr);
                    String binary=this.hash(key);
                    Map<Integer,String[]> sameBinDir=sameBinaryDir(binary.substring(binary.length()-compareBit));
                    for (Integer k:pointToEmptyBucket.keySet()) {
                        sameBinDir.remove(k);
                    }if(!sameBinDir.isEmpty()){//have the similar bir can combine with pointToEmptyBucket
                        long addr=Long.valueOf("0");
                        for (Map.Entry<Integer,String[]> e: sameBinDir.entrySet()) {
                            if(addr==0){
                                addr=Long.valueOf(e.getValue()[1]);
                            }else if(addr!=Long.valueOf(e.getValue()[1])){
                                return result;
                            }
                        }for (Map.Entry<Integer,String[]> e: pointToEmptyBucket.entrySet()) {
                            e.getValue()[1]=String.format("% 6d",addr);
                            this.directory.seek(Long.valueOf(e.getValue()[0]));
                            this.directory.writeBytes(e.getValue()[0]+';'+e.getValue()[1]);
                            bucket.isIndex=true;
                            bucket.count=0;
                        }Bucket pointTo=new Bucket(addr);
                        pointTo.bucketBits--;
                        this.buckets.seek(addr);
                        this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+pointTo.toString());
                    }
                }this.buckets.seek(bucketAddr);
                this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+bucket.toString()+'\n');
                if(bucket.isIndex){
                    this.buckets.seek(this.getBucketFree(this.bucketFree,Long.valueOf("0")));
                    int numOfSpace=bucketSize*14+13;
                    this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+String.format("% "+numOfSpace+"d",bucketAddr));
                }int maxBucketBit=getMaxBucketBit();
                while(directoryBIts-1>=maxBucketBit && directoryBIts>1){//the dir can be shirmp
                    int counter=0;
                    this.directory.seek(0);
                    this.directory.readLine();
                    Long cut;
                    while(this.directory.getFilePointer()<this.directory.length()){
                        cut=this.directory.getFilePointer();
                        if(hash(counter).charAt(0)=='1'){
                            this.directory.setLength(cut);
                            this.directory.seek(0);
                            this.directory.writeBytes(String.valueOf(this.directory.getFilePointer())+';'+String.format("% 6d",--directoryBIts));
                            break;
                        }else {
                            this.directory.readLine();
                            counter++;
                        }
                    }
                }return result;
            }catch (IOException e){
                e.printStackTrace();
            }
        }return deleteDBAddr;
    }

    private int getMaxBucketBit(){
        int result=0;
        try{
            this.buckets.seek(Long.valueOf("0"));
            while(this.buckets.getFilePointer()<this.buckets.length()){
                String[] bucketBit=this.buckets.readLine().replace(" ","").split(";");
                if(result<Integer.valueOf(bucketBit[1]) && bucketBit.length>2){
                    result=Integer.valueOf(bucketBit[1]);
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }return result;
    }

    private Map<Integer,String[]> pointSameBucket(long addr){//传入String
        Map<Integer,String[]> result=new HashMap<>();
        try{
            this.directory.seek(0);
            int counter=-1;
            while(this.directory.getFilePointer()<this.directory.length()){
                String[] tuple=this.directory.readLine().split(";");
                if(Long.valueOf(tuple[1].replace(" ","")).equals(addr)){
                    result.put(counter,tuple);
                }counter++;
            }return result;
        }catch (IOException e){
            e.printStackTrace();
        }return result;
    }

    private Map<Integer, String[]> sameBinaryDir(String binary){
        Map<Integer,String[]> result=new HashMap<>();
        try{
            this.directory.seek(0);
            this.directory.readLine();
            int counter=0;
            while(this.directory.getFilePointer()<this.directory.length()){
                String[] tuple=this.directory.readLine().replace(" ","").split(";");
                String hashCode=this.hash(counter);
                if(hashCode.substring(hashCode.length()-binary.length()).equals(binary)){
                    result.put(counter,tuple);
                }counter++;
            }return result;
        }catch (IOException e){
            e.printStackTrace();
        }return result;
    }

    private Long getBucketFree(Long addr,Long dest){
        try{
            this.buckets.seek(addr);
            String []tuple=this.buckets.readLine().split(";");
            Long pointTo=Long.parseLong(tuple[1].replace(" ",""));
            if(pointTo.equals(dest)){
                return addr;
            }else {
                return this.getBucketFree(pointTo,dest);
            }
        }catch (IOException e){
            e.printStackTrace();
        }return addr;
    }

    private Long getInsertAddr(){
        Long addr=getBucketFree(this.bucketFree,Long.valueOf("0"));
        try{
            if(addr==this.bucketFree){
                this.buckets.seek(addr);
                int counter=this.bucketSize*14+13;
                this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+String.format("% "+counter+"d",this.buckets.length())+'\n');
                Long result=this.buckets.length();
                this.buckets.seek(this.buckets.length());
                this.buckets.writeBytes(String.valueOf(this.buckets.getFilePointer())+';'+String.format("% "+counter+"d",0)+'\n');
                return result;
            }else {
                return addr;
            }
        }catch (IOException e){
            e.printStackTrace();
        }return addr;
    }

    public long search(int key){
        try{
            this.directory.seek(getDir(key));
            Bucket bucket=new Bucket(Long.valueOf(this.directory.readLine().split(";")[1].replace(" ","")));
            return bucket.searchKey(key);
        }catch (IOException e){
            e.printStackTrace();
        }return Long.valueOf("0");
    }

    private long getDir(int key){
        try{
            String hash=this.hash(key);
            int counter=0;
            for(int i=0;i<hash.length();i++){
                counter=counter*2+Integer.valueOf(String.valueOf(hash.charAt(i)));
            }counter++;
            this.directory.seek(0);
            while(counter>0){
                this.directory.readLine();
                counter--;
            }return this.directory.getFilePointer();
        }catch (IOException e){
            e.printStackTrace();
        }return Long.valueOf("0");
    }

    private String hash(int key) {
        String result=Integer.toBinaryString(key);
        if(result.length()<this.directoryBIts){
            result=String.format("%0"+this.directoryBIts+"d",0)+result;
        }return result.substring(result.length()-this.directoryBIts);
    }

    private boolean ifHashEqual(int key1,int key2){
        String result1=Integer.toBinaryString(key1);
        String result2=Integer.toBinaryString(key2);
        if(result1.length()<this.maxLengthOfHash){
            int numOf0=this.maxLengthOfHash-result1.length();
            result1=String.format("%0"+numOf0+"d",0)+result1;
        }else {
            result1=result1.substring(result1.length()-this.maxLengthOfHash);
        }if(result2.length()<this.maxLengthOfHash){
            int numOf0=this.maxLengthOfHash-result2.length();
            result2=String.format("%0"+numOf0+"d",0)+result2;
        }else {
            result2=result2.substring(result2.length()-this.maxLengthOfHash);
        }if(result1.equals(result2)){
//            System.out.print("the key1 is: "+key1+" and hash value is: "+result1+'\n');
//            System.out.print("the key2 is: "+key2+"and hash value is: "+result2+'\n');
//            System.out.print("----------------------------------------"+'\n');
        }return result1.equals(result2);
    }

    public void close() {
        try{
            if(this.buckets!=null && this.directory!=null && this.extendBucket!=null){
                this.directory.seek(0);
                this.buckets.close();
                this.directory.close();
                this.extendBucket.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
