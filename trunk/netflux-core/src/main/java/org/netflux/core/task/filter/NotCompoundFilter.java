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

import org.netflux.core.Record;

/**
 * Filter that accepts a record if the provided filter doesn't accept it.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public class NotCompoundFilter implements Filter
  {
  private Filter filter;

  /**
   * Returns the filter used to compute acceptance or rejection of records by this filter.
   * 
   * @return the filter used by this filter.
   */
  public Filter getFilter( )
    {
    return filter;
    }

  /**
   * Sets the filter used to compute acceptance or rejection of records by this filter.
   * 
   * @param filters the filter used by this filter.
   */
  public void setFilter( Filter filter )
    {
    this.filter = filter;
    }

  /**
   * Returns <code>true</code> if the provided filter doesn't accept the record, <code>false</code> otherwise.
   * 
   * @param record the record to be accepted or rejected by this filter.
   * @return <code>true</code> if the provided filter doesn't accept the record, <code>false</code> otherwise.
   */
  public boolean accepts( Record record )
    {
    return !this.getFilter( ).accepts( record );
    }
  }
