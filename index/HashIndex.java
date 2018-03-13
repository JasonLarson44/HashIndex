package index;

import global.*;

/**
 * <h3>Minibase Hash Index</h3>
 * This unclustered index implements static hashing as described on pages 371 to
 * 373 of the textbook (3rd edition).  The index file is a stored as a heapfile.  
 */
public class HashIndex implements GlobalConst {

  /** File name of the hash index. */
  protected String fileName;

  /** Page id of the directory. */
  protected PageId headId;
  
  //Log2 of the number of buckets - fixed for this simple index
  protected final int  DEPTH = 7;

  // --------------------------------------------------------------------------

  /**
   * Opens an index file given its name, or creates a new index file if the name
   * doesn't exist; a null name produces a temporary index file which requires
   * no file library entry and whose pages are freed when there are no more
   * references to it.
   * The file's directory contains the locations of the 128 primary bucket pages.
   * You will need to decide on a structure for the directory.
   * The library entry contains the name of the index file and the pageId of the
   * file's directory.
   */
  public HashIndex(String fileName) {

      PageId pId = Minibase.DiskManager.get_file_entry(fileName);
      this.fileName = fileName;
      if(pId != null) //index was already in the file library
      {
          this.headId = pId; //Set this page id to be the page id of the located file
      }
      else //File was not in the file library
      {
          this.headId = Minibase.DiskManager.allocate_page();
          HashDirPage IndexHead = new HashDirPage();
          Minibase.BufferManager.pinPage(this.headId, IndexHead, GlobalConst.PIN_MEMCPY);
          Minibase.BufferManager.unpinPage(this.headId, true);
          if(this.fileName != null)//Not a temp file, so add it to the file library
          {
              Minibase.DiskManager.add_file_entry(this.fileName, this.headId);
          }
      }

  } // public HashIndex(String fileName)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the index file if it's temporary.
   */
  protected void finalize() throws Throwable {

	  if(this.fileName == null) //temp
      {
          deleteFile();
      }

  } // protected void finalize() throws Throwable

   /**
   * Deletes the index file from the database, freeing all of its pages.
   */
  public void deleteFile() {
      PageId bucketId;
      HashDirPage dirPage = new HashDirPage();
      Minibase.BufferManager.pinPage(this.headId, dirPage, GlobalConst.PIN_DISKIO); //pin the directory page

      for(int i = 0; i < 128; ++i)//for each bucket
      {
          bucketId = dirPage.getPageId(i); //get the page id at each slot
          if(bucketId.pid >= 0){//if bucket is allocated
              freeBucket(bucketId); //free all pages in the bucket
          }
      }
      Minibase.BufferManager.unpinPage(this.headId, UNPIN_CLEAN);
      if(this.fileName != null)//if not a temp file
      {
          //remove from the file library
          Minibase.DiskManager.delete_file_entry(fileName);
      }
      Minibase.BufferManager.freePage(this.headId); //free the page
      this.headId.pid = -1;

  } // public void deleteFile()

  /**
   * Inserts a new data entry into the index file.
   * 
   * @throws IllegalArgumentException if the entry is too large
   */
  public void insertEntry(SearchKey key, RID rid) {

      if(key.getLength() > PAGE_SIZE)
      {
          throw new IllegalArgumentException("Invalid entry");
      }

      HashDirPage dirPage = new HashDirPage();
      HashBucketPage bucket = new HashBucketPage();
      PageId bucketId;
      DataEntry entry = new DataEntry(key, rid); //create a data entry to insert into bucket
      boolean isDirty = false;

	  int hashResult = key.getHash(DEPTH);
	  Minibase.BufferManager.pinPage(this.headId, dirPage, GlobalConst.PIN_DISKIO); //pin the directory page
      bucketId = dirPage.getPageId(hashResult);//find the page
      if(bucketId.pid < 0)
      {//invalid or unallocated bucket
          bucketId = Minibase.DiskManager.allocate_page(); //allocate a new page
          dirPage.setPageId(hashResult, bucketId);
          Minibase.BufferManager.unpinPage(this.headId, UNPIN_DIRTY);
          Minibase.BufferManager.pinPage(bucketId, bucket, GlobalConst.PIN_MEMCPY); //copy defaults in
          isDirty = bucket.insertEntry(entry);
          Minibase.BufferManager.unpinPage(bucketId, isDirty);
      }
      else { //if we found a valid bucket
          Minibase.BufferManager.unpinPage(this.headId, UNPIN_CLEAN);
          Minibase.BufferManager.pinPage(bucketId, bucket, GlobalConst.PIN_DISKIO);
          isDirty = bucket.insertEntry(entry);
          Minibase.BufferManager.unpinPage(bucketId, isDirty);
      }
  } // public void insertEntry(SearchKey key, RID rid)

  /**
   * Deletes the specified data entry from the index file.
   * 
   * @throws IllegalArgumentException if the entry doesn't exist
   */
  public void deleteEntry(SearchKey key, RID rid) {

	  HashDirPage dirPage = new HashDirPage();
	  PageId bucketId;
	  HashBucketPage bucket = new HashBucketPage();
	  boolean isDirty;
	  DataEntry entry = new DataEntry(key, rid); //create the data entry to pass to bucket

	  Minibase.BufferManager.pinPage(this.headId, dirPage, GlobalConst.PIN_DISKIO); //pin the directory page
      int hash = key.getHash(DEPTH);
      bucketId = dirPage.getPageId(hash); //find the right bucket
      Minibase.BufferManager.unpinPage(this.headId, GlobalConst.UNPIN_CLEAN); //bucketpages dont get deleted so unpin is always clean
      Minibase.BufferManager.pinPage(bucketId, bucket, GlobalConst.PIN_DISKIO);
      isDirty = bucket.deleteEntry(entry);

      Minibase.BufferManager.unpinPage(bucketId, isDirty); //if the entry was on the main bucket page it will be dirty

  } // public void deleteEntry(SearchKey key, RID rid)

  /**
   * Initiates an equality scan of the index file.
   */
  public HashScan openScan(SearchKey key) {
    return new HashScan(this, key);
  }

  /**
   * Returns the name of the index file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Prints a high-level view of the directory, namely which buckets are
   * allocated and how many entries are stored in each one. Sample output:
   * 
   * <pre>
   * IX_Customers
   * ------------
   * 0000000 : 35
   * 0000001 : null
   * 0000010 : 27
   * ...
   * 1111111 : 42
   * ------------
   * Total : 1500
   * </pre>
   */
  public void printSummary() {

      int numEntries;
      PageId bucketId;
      HashBucketPage bucket = new HashBucketPage();
      HashDirPage dirPage = new HashDirPage();

      Minibase.BufferManager.pinPage(this.headId, dirPage, GlobalConst.PIN_DISKIO);

	  System.out.print("<pre>\n");
	  System.out.print(this.fileName);
	  System.out.print("\n-----------\n");

	  for(int i = 0; i < 128; ++i) //number of buckets is fixed for this index
      {
          bucketId = dirPage.getPageId(i);
          System.out.print(Integer.toBinaryString(i));
          System.out.print(" : ");
          if(bucketId.pid < 0) // bucket not allocated
          {
              System.out.print("null\n");
          }
          else
          {
              Minibase.BufferManager.pinPage(bucketId, bucket, GlobalConst.PIN_DISKIO);
              System.out.print(bucket.countEntries());
              System.out.print("\n");
              Minibase.BufferManager.unpinPage(bucketId, GlobalConst.UNPIN_CLEAN);
          }
      }
      Minibase.BufferManager.unpinPage(this.headId, UNPIN_CLEAN);

  } // public void printSummary()

    //frees all the overflow pages for a bucket
  private void freeBucket(PageId bucket)
  {
      PageId currentId = bucket;
      HashBucketPage currentPage = new HashBucketPage();

      while(currentId.pid >= 0){
          Minibase.BufferManager.pinPage(currentId, currentPage, GlobalConst.PIN_DISKIO);
          Minibase.BufferManager.unpinPage(currentId, GlobalConst.UNPIN_CLEAN);
          Minibase.BufferManager.freePage(currentId);//free the page
          currentId = currentPage.getNextPage();//get the next page id
      }
  }

} // public class HashIndex implements GlobalConst
