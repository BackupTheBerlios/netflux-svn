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
package org.netflux.core.task.filter;

import java.io.Serializable;

import org.netflux.core.Record;

/**
 * Filter that accepts a record if the values contained in the supplied fields are equal.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class EqualToFieldFilter extends AbstractToFieldLogicFilter
  {
  /**
   * Returns <code>true</code> if the value contained in the first field is equal to the value contained in the second field,
   * <code>false</code> otherwise.
   * 
   * @param record the record to be accepted or rejected by this filter.
   * @return <code>true</code> if the value contained in the first field is equal to the value contained in the second field,
   *         <code>false</code> otherwise.
   */
  public boolean accepts( Record record )
    {
    Serializable firstValue = record.getValue( Serializable.class, this.getFieldName( ) );
    Serializable secondValue = record.getValue( Serializable.class, this.getOtherFieldName( ) );
    if( firstValue == null )
      {
      return secondValue == null;
      }
    else
      {
      return firstValue.equals( secondValue );
      }
    }
  }
