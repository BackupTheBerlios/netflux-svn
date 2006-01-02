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

/**
 * A path where records may flow through. The expected behavior of a class implementing this interface is to broadcast every record
 * received using the {@link RecordSink#consume(Record)} method to all the registered {@link RecordSink}s. No transformation or
 * filtering should be done on records, as this kind of behavior is expected to be placed in {@link Task}s. However, a
 * <code>Channel</code> may do intermediate buffering or storage, logging or any other operation that doesn't modify the records
 * themselves.
 * 
 * @author OPEN input - <a href="http://www.openinput.com/">http://www.openinput.com/</a>
 */
public interface Channel extends RecordSource, RecordSink
  {}
