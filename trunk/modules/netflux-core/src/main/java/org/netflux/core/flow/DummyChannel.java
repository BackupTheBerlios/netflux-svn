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
package org.netflux.core.flow;

import java.beans.PropertyChangeListener;

import org.netflux.core.Record;
import org.netflux.core.RecordMetadata;
import org.netflux.core.RecordSink;

/**
 * @author jgonzalez
 */
public class DummyChannel implements RecordSink
  {
  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#setMetadata(org.netflux.core.RecordMetadata)
   */
  public void setMetadata( RecordMetadata metadata )
    {}

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#consume(org.netflux.core.Record)
   */
  public void consume( Record object )
    {}

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#addPropertyChangeListener(java.beans.PropertyChangeListener)
   */
  public void addPropertyChangeListener( PropertyChangeListener listener )
    {}

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.RecordSink#removePropertyChangeListener(java.beans.PropertyChangeListener)
   */
  public void removePropertyChangeListener( PropertyChangeListener listener )
    {}
  }