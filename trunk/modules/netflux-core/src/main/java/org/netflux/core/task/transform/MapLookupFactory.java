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
package org.netflux.core.task.transform;

import java.util.Map;

/**
 * @author jgonzalez
 */
public class MapLookupFactory implements LookupTableFactory
  {
  private Map<? extends Object, ? extends Object> lookupTable;

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.LookupTableFactory#getLookupTable()
   */
  public Map<? extends Object, ? extends Object> getLookupTable( )
    {
    return this.lookupTable;
    }

  /**
   * @param lookupTable The lookupTable to set.
   */
  public void setLookupTable( Map<? extends Object, ? extends Object> lookupTable )
    {
    this.lookupTable = lookupTable;
    }
  }
