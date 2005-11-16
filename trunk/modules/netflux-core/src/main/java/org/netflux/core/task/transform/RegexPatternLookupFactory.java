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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author jgonzalez
 */
public class RegexPatternLookupFactory implements LookupTableFactory
  {
  private Map<? extends Object, ? extends Object> lookupTable;
  private List<String>                            patterns;
  private int                                     flags;
  private List<? extends Object>                  values;

  /*
   * (non-Javadoc)
   * 
   * @see org.netflux.core.task.transform.LookupTableFactory#getLookupTable()
   */
  public Map<? extends Object, ? extends Object> getLookupTable( )
    {
    if( this.lookupTable == null )
      {
      this.lookupTable = new RegexPatternMap<Object>( this.patterns, this.values, this.flags );
      }
    return this.lookupTable;
    }

  /**
   * @return Returns the patterns.
   */
  public List<String> getPatterns( )
    {
    return this.patterns;
    }

  /**
   * @param patterns The patterns to set.
   */
  public void setPatterns( List<String> patterns )
    {
    this.patterns = patterns;
    }

  /**
   * @return Returns the flags.
   */
  public int getFlags( )
    {
    return this.flags;
    }

  /**
   * @param flags The flags to set.
   */
  public void setFlags( int flags )
    {
    this.flags = flags;
    }

  /**
   * @return Returns the values.
   */
  public List<? extends Object> getValues( )
    {
    return this.values;
    }

  /**
   * @param values The values to set.
   */
  public void setValues( List<? extends Object> values )
    {
    this.values = values;
    }

  /**
   * @author jgonzalez
   * @param <V>
   */
  protected class RegexPatternMap<V> implements Map<String, V>
    {
    private LinkedHashMap<Pattern, V> patternMap = new LinkedHashMap<Pattern, V>( );

    /**
     * 
     */
    public RegexPatternMap( List<String> patterns, List<? extends V> values, int flags )
      {
      if( patterns.size( ) == values.size( ) )
        {
        Iterator<String> stringPatternIterator = patterns.iterator( );
        Iterator<? extends V> valueIterator = values.iterator( );
        while( stringPatternIterator.hasNext( ) )
          {
          String stringPattern = stringPatternIterator.next( );
          V value = valueIterator.next( );
          this.patternMap.put( Pattern.compile( stringPattern, flags ), value );
          }
        }
      else
        {
        throw new IllegalArgumentException( );
        }
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#size()
     */
    public int size( )
      {
      return this.patternMap.size( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty( )
      {
      return this.patternMap.isEmpty( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey( Object key )
      {
      for( Pattern pattern : this.patternMap.keySet( ) )
        {
        if( pattern.matcher( key.toString( ) ).matches( ) ) { return true; }
        }
      return false;
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue( Object value )
      {
      return this.containsValue( value );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#get(java.lang.Object)
     */
    public V get( Object key )
      {
      for( Pattern pattern : this.patternMap.keySet( ) )
        {
        if( pattern.matcher( key.toString( ) ).matches( ) ) { return this.patternMap.get( pattern ); }
        }
      return null;
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#put(K, V)
     */
    public V put( String key, V value )
      {
      throw new UnsupportedOperationException( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#remove(java.lang.Object)
     */
    public V remove( Object key )
      {
      throw new UnsupportedOperationException( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll( Map<? extends String, ? extends V> t )
      {
      throw new UnsupportedOperationException( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#clear()
     */
    public void clear( )
      {
      throw new UnsupportedOperationException( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#keySet()
     */
    public Set<String> keySet( )
      {
      Set<String> keySet = new LinkedHashSet<String>( );
      for( Pattern pattern : this.patternMap.keySet( ) )
        {
        keySet.add( pattern.pattern( ) );
        }
      return keySet;
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#values()
     */
    public Collection<V> values( )
      {
      return this.patternMap.values( );
      }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Map#entrySet()
     */
    public Set<Map.Entry<String, V>> entrySet( )
      {
      Set<Map.Entry<String, V>> entrySet = new LinkedHashSet<Map.Entry<String, V>>( );
      for( Map.Entry<Pattern, V> entry : this.patternMap.entrySet( ) )
        {
        entrySet.add( new Entry( entry.getKey( ).pattern( ), entry.getValue( ) ) );
        }
      return entrySet;
      }

    private class Entry implements Map.Entry<String, V>
      {
      private String key;
      private V      value;

      /**
       * 
       */
      public Entry( String key, V value )
        {
        this.key = key;
        this.value = value;
        }

      /*
       * (non-Javadoc)
       * 
       * @see java.util.Map.Entry#getKey()
       */
      public String getKey( )
        {
        return this.key;
        }

      /*
       * (non-Javadoc)
       * 
       * @see java.util.Map.Entry#getValue()
       */
      public V getValue( )
        {
        return this.value;
        }

      /*
       * (non-Javadoc)
       * 
       * @see java.util.Map.Entry#setValue(V)
       */
      public V setValue( V value )
        {
        throw new UnsupportedOperationException( );
        }

      /*
       * (non-Javadoc)
       * 
       * @see java.lang.Object#equals(java.lang.Object)
       */
      @Override
      public boolean equals( Object o )
        {
        if( o instanceof Map.Entry )
          {
          Map.Entry<?, ?> otherMapEntry = (Map.Entry<?, ?>) o;
          return (this.getKey( ) == null ? otherMapEntry.getKey( ) == null : this.getKey( ).equals( otherMapEntry.getKey( ) ))
              && (this.getValue( ) == null ? otherMapEntry.getValue( ) == null : this.getValue( ).equals( otherMapEntry.getValue( ) ));
          }
        else
          {
          return false;
          }
        }

      /*
       * (non-Javadoc)
       * 
       * @see java.lang.Object#hashCode()
       */
      @Override
      public int hashCode( )
        {
        return (this.getKey( ) == null ? 0 : this.getKey( ).hashCode( ))
            ^ (this.getValue( ) == null ? 0 : this.getValue( ).hashCode( ));
        }
      }
    }
  }
