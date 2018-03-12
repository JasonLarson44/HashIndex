package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;

/**
 * An object in this class is a page in a linked list.
 * The entire linked list is a hash table bucket.
 */
class HashBucketPage extends SortedPage {

  /**
   * Gets the number of entries in this page and later
   * (overflow) pages in the list.
   * <br><br>
   * To find the number of entries in a bucket, apply 
   * countEntries to the primary page of the bucket.
   */
  public int countEntries() {

      int count = this.getEntryCount(); //get number of entries on this page
	  PageId nextID = this.getNextPage(); //check if there are more pages
	  HashBucketPage nextPage = new HashBucketPage();
	  if(nextID.pid > 0) //if more pages
	  {//pin the next hashbucket page and recursively call countEntries until end of overflow list
          Minibase.BufferManager.pinPage(nextID, nextPage, GlobalConst.PIN_DISKIO );
          Minibase.BufferManager.unpinPage(nextID, GlobalConst.UNPIN_CLEAN);
          count += nextPage.countEntries();
      }
      return count;
  } // public int countEntries()

  /**
   * Inserts a new data entry into this page. If there is no room
   * on this page, recursively inserts in later pages of the list.  
   * If necessary, creates a new page at the end of the list.
   * Does not worry about keeping order between entries in different pages.
   * <br><br>
   * To insert a data entry into a bucket, apply insertEntry to the
   * primary page of the bucket.
   * 
   * @return true if inserting made this page dirty, false otherwise
   */
  public boolean insertEntry(DataEntry entry) {
      boolean myStatus = false;
      boolean recursiveStatus;
      PageId nextId;
      HashBucketPage nextPage = new HashBucketPage();

      try {myStatus = insertEntrySuper(entry);}
      catch (IllegalStateException exc){
          //Insufficient space to insert, try to insert on next page
          nextId = this.getNextPage();
          if(nextId.pid > 0)//try to add on next page, else there is no more pages, create a new one
          {
              Minibase.BufferManager.pinPage(nextId, nextPage, GlobalConst.PIN_DISKIO);
              recursiveStatus = nextPage.insertEntrySuper(entry);
              Minibase.BufferManager.unpinPage(nextId, recursiveStatus);
          }
          else //no more space, add a new overflow page
          {
              nextId = Minibase.DiskManager.allocate_page();
              Minibase.BufferManager.pinPage(nextId, nextPage, GlobalConst.PIN_DISKIO);

              //pin the page and link it to the list
              nextPage.initDefaults();
              this.setNextPage(nextId);

              myStatus = nextPage.insertEntrySuper(entry); //insert the entry
              Minibase.BufferManager.unpinPage(nextId, myStatus); //my status because we had to change this page's ptr
          }
      }

      return myStatus;
  } // public boolean insertEntry(DataEntry entry)

  private boolean insertEntrySuper(DataEntry entry){
      boolean status;
      try{status = super.insertEntry(entry);}
      catch(IllegalStateException exc){
          throw new IllegalStateException();
      }
      return status;
  }

  /**
   * Deletes a data entry from this page.  If a page in the list 
   * (not the primary page) becomes empty, it is deleted from the list.
   * 
   * To delete a data entry from a bucket, apply deleteEntry to the
   * primary page of the bucket.
   * 
   * @return true if deleting made this page dirty, false otherwise
   * @throws IllegalArgumentException if the entry is not in the list.
   */
  public boolean deleteEntry(DataEntry entry) {
      boolean status = true;
      PageId nextId;
      HashBucketPage nextPage = new HashBucketPage();

      try{ status = deleteEntrySuper(entry);}
      catch (IllegalArgumentException exc){ //entry isnt on the page
          nextId = getNextPage();
          if(nextId.pid > 0){
              Minibase.BufferManager.pinPage(nextId, nextPage, GlobalConst.PIN_DISKIO);
              status = nextPage. deleteEntryInList(entry);
              Minibase.BufferManager.unpinPage(nextId, status);
          }
          else //throw an exception
          throw new IllegalArgumentException("The entry is not in this hashbucket");
      }

      return status;
  }// public boolean deleteEntry(DataEntry entry)

    /**
    *This function is only called by the deleteEntry function above. The deleteEntry function is used
     * as a wrapper to first check if the entry is on the primary page, if it is then delete it from that page.
     * if it is not on that page, the deleteEntryInList function is called on the first overflow page and recursively
     * searches for the entry, if the page becomes empty, delete it.
     */
  private boolean deleteEntryInList(DataEntry entry){
      boolean status;
      PageId nextId;
      HashBucketPage nextPage = new HashBucketPage();

      try{ status = deleteEntrySuper(entry);}
      catch (IllegalArgumentException exc){ //entry isnt on the page
          nextId = getNextPage();
          if(nextId.pid > 0){
              Minibase.BufferManager.pinPage(nextId, nextPage, GlobalConst.PIN_DISKIO);
              status = nextPage. deleteEntryInList(entry);
              Minibase.BufferManager.unpinPage(nextId, status);

              if(nextPage.getEntryCount() == 0){//If the next page is empty delete it
                  this.setNextPage(nextPage.getNextPage());
                  Minibase.DiskManager.deallocate_page(nextId);
              }
          }
          else //throw an exception
              throw new IllegalArgumentException("The entry is not in this hashbucket");

      }

      return status;


  } // public boolean deleteEntryInList(DataEntry entry)

  private boolean deleteEntrySuper(DataEntry entry){
      boolean status;
      try{ status = super.deleteEntry(entry);}
      catch (IllegalArgumentException exc)
      {
         throw new IllegalArgumentException();
      }
      return status;
  }

} // class HashBucketPage extends SortedPage
