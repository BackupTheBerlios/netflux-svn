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
package org.netflux.core;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * Thrown to indicate that a method has been passed a record with a metadata different than expected.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class InvalidRecordMetadataException extends IllegalArgumentException
  {
  private static final long     serialVersionUID = -805534418627047765L;
  private static ResourceBundle messages         = ResourceBundle.getBundle( InvalidRecordMetadataException.class.getName( ) );

  private Record                record;
  private RecordMetadata        expectedMetadata;

  /**
   * Constructs a <code>InvalidRecordMetadataException</code> with the specified offending <code>record</code> and expected
   * metadata.
   * 
   * @param record the record causing the exception.
   * @param expectedMetadata the expected metadata the record didn't comply with.
   */
  public InvalidRecordMetadataException( Record record, RecordMetadata expectedMetadata )
    {
    super( MessageFormat.format( InvalidRecordMetadataException.messages.getString( "message.invalid.metadata" ), record.getMetadata( )
        .toString( ), expectedMetadata.toString( ), record.toString( ) ) );
    this.record = record;
    this.expectedMetadata = expectedMetadata;
    }

  /**
   * @return Returns the record.
   */
  public Record getRecord( )
    {
    return this.record;
    }

  /**
   * @return Returns the expectedMetadata.
   */
  public RecordMetadata getExpectedMetadata( )
    {
    return this.expectedMetadata;
    }
  }
