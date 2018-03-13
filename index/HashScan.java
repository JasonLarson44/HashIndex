package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;

/**
 * A HashScan retrieves all records with a given key (via the RIDs of the records).  
 * It is created only through the function openScan() in the HashIndex class. 
 */
public class HashScan implements GlobalConst {

  /** The search key to scan for. */
  protected SearchKey key;

  /** Id of HashBucketPage being scanned. */
  protected PageId curPageId;

  /** HashBucketPage being scanned. */
  protected HashBucketPage curPage;

  /** Current slot to scan from. */
  protected int curSlot;

  // --------------------------------------------------------------------------

  /**
   * Constructs an equality scan by initializing the iterator state.
   */
  protected HashScan(HashIndex index, SearchKey key) {


      HashDirPage dirPage = new HashDirPage();

	  this.key = new SearchKey(key);
	  int hash = key.getHash(index.DEPTH);
	  if(index.headId.pid >= 0) {
          Minibase.BufferManager.pinPage(index.headId, dirPage, GlobalConst.PIN_DISKIO);
          this.curPageId = dirPage.getPageId(hash);//find the bucket the key hashes to
          Minibase.BufferManager.unpinPage(index.headId, GlobalConst.UNPIN_CLEAN);
          this.curPage = new HashBucketPage();
          this.curSlot = -1;
      }
      if (this.curPageId.pid >= 0)
      {
          Minibase.BufferManager.pinPage(this.curPageId, this.curPage, GlobalConst.PIN_DISKIO); //pin the first bucket page
      }
      else
      {
          this.curPage = null;
          this.curPageId = new PageId();
          this.curSlot = -1;
      }
  } // protected HashScan(HashIndex index, SearchKey key)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
      if(this.curPageId.pid != -1)
      {
          close();
      }

  } // protected void finalize() throws Throwable

  /**
   * Closes the index scan, releasing any pinned pages.
   */
  public void close() {
      if(this.curPageId.pid >= 0)
      {
          Minibase.BufferManager.unpinPage(curPageId, GlobalConst.UNPIN_CLEAN);
          this.curPageId.pid = -1;
      }

  } // public void close()

   /**
   * Gets the next entry's RID in the index scan.
   * 
   * @throws IllegalStateException if the scan has no more entries
   */
  public RID getNext() {

      RID rid;
      if(curPage == null) {
          return null;
      }
	  int slotno = curPage.nextEntry(this.key, this.curSlot);
	  if(slotno != -1) //found entry
      {
        this.curSlot = slotno;
        rid = new RID(curPage.getEntryAt(slotno).rid);
        return rid;
      }
      else //else check each overflow page
      {
          Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);//unpin the page first
          curPageId = curPage.getNextPage();
          while(curPageId.pid >= 0)//while a valid page id
          {
              Minibase.BufferManager.pinPage(curPageId, curPage, GlobalConst.PIN_DISKIO);
              this.curSlot = 0; //start at first slot
              slotno = curPage.nextEntry(key, curSlot);
              if(slotno != -1) //found entry
              {
                  this.curSlot = slotno;
                  rid = new RID(curPageId, slotno);
                  return rid;
              }
              Minibase.BufferManager.unpinPage(curPageId, GlobalConst.UNPIN_CLEAN);
              curPageId = curPage.getNextPage();
          }
          //throw new IllegalStateException("There are no more entries");

      }
      return null;
  } // public RID getNext()

} // public class HashScan implements GlobalConst
