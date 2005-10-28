/* 
 * netflux-core - Copyright (C) 2005 OPEN input - http://www.openinput.com/
 *
 * This program is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU General Public License as published by the 
 * Free Software Foundation; either version 2 of the License, or (at your 
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. 
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along 
 * with this program; if not, write to 
 *   the Free Software Foundation, Inc., 
 *   59 Temple Place, Suite 330, 
 *   Boston, MA 02111-1307 USA
 *   
 * $Id$
 */
package org.netflux.core.source;

import org.netflux.core.Record;

/**
 * @author jgonzalez
 */
public class SourceDataStorageException extends Exception
  {
  private static final long serialVersionUID = -3489891842797439485L;
  
  protected Record          partialRecord;
  protected String          source;
  protected boolean         fatal;

  /**
   * @param message
   * @param partialRecord
   * @param source
   */
  public SourceDataStorageException( String message, Record partialRecord, String source )
    {
    this( message, partialRecord, source, false );
    }

  /**
   * @param message
   * @param partialRecord
   * @param source
   * @param throwable
   */
  public SourceDataStorageException( String message, Record partialRecord, String source, Throwable throwable )
    {
    this( message, partialRecord, source, false, throwable );
    }

  /**
   * @param message
   * @param partialRecord
   * @param source
   * @param fatal
   */
  public SourceDataStorageException( String message, Record partialRecord, String source, boolean fatal )
    {
    super( message );
    this.partialRecord = partialRecord;
    this.source = source;
    this.fatal = fatal;
    }

  /**
   * @param message
   * @param partialRecord
   * @param source
   * @param fatal
   * @param throwable
   */
  public SourceDataStorageException( String message, Record partialRecord, String source, boolean fatal, Throwable throwable )
    {
    super( message, throwable );
    this.partialRecord = partialRecord;
    this.source = source;
    this.fatal = fatal;
    }

  /**
   * @return Returns the partialRecord.
   */
  public Record getPartialRecord( )
    {
    return this.partialRecord;
    }

  /**
   * @return Returns the source.
   */
  public String getSource( )
    {
    return this.source;
    }

  /**
   * @return Returns the fatal.
   */
  public boolean isFatal( )
    {
    return this.fatal;
    }
  }
