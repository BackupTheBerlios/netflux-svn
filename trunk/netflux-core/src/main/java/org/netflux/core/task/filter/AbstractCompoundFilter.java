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

import java.util.List;

/**
 * Abstract base class for compound filters, this is, filters that accept or reject records based on the acceptance or rejection of a
 * list of provided filters.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public abstract class AbstractCompoundFilter implements Filter
  {
  private List<Filter> filters;

  /**
   * Returns the list of filters used to compute acceptance or rejection of records by this filter.
   * 
   * @return the list of filters used by this filter.
   */
  public List<Filter> getFilters( )
    {
    return filters;
    }

  /**
   * Sets the list of filters used to compute acceptance or rejection of records by this filter.
   * 
   * @param filters the list of filters used by this filter.
   */
  public void setFilters( List<Filter> filters )
    {
    this.filters = filters;
    }
  }
