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
package org.netflux.core.task.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.netflux.core.Record;

/**
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class RecordComparator implements Comparator<Record>
  {
  private List<String> key;

  /**
   * 
   */
  public RecordComparator( List<String> key )
    {
    this.key = new ArrayList<String>( key );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Comparator#compare(T, T)
   */
  public int compare( Record firstRecord, Record secondRecord )
    {
    // TODO Auto-generated method stub
    if( this.key == null || this.key.isEmpty( ) )
      {
      return firstRecord.compareTo( secondRecord );
      }
    else
      {
      return firstRecord.extract( this.key ).compareTo( secondRecord.extract( this.key ) );
      }
    }
  }
