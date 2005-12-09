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
package org.netflux.core.sink.database;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jgonzalez
 */
class BindablePreparedStatement implements PreparedStatement
  {
  private PreparedStatement          preparedStatement;
  private Map<String, List<Integer>> parameterMapping;

  /**
   * @param preparedStatement
   * @param parameterMapping
   */
  private BindablePreparedStatement( PreparedStatement preparedStatement, Map<String, List<Integer>> parameterMapping )
    {
    this.preparedStatement = preparedStatement;
    this.parameterMapping = parameterMapping;
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#executeQuery()
   */
  public ResultSet executeQuery( ) throws SQLException
    {
    return this.preparedStatement.executeQuery( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#executeUpdate()
   */
  public int executeUpdate( ) throws SQLException
    {
    return this.preparedStatement.executeUpdate( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setNull(int, int)
   */
  public void setNull( int parameterIndex, int sqlType ) throws SQLException
    {
    this.preparedStatement.setNull( parameterIndex, sqlType );
    }

  /**
   * @param parameterName
   * @param sqlType
   * @throws SQLException
   */
  public void setNull( String parameterName, int sqlType ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setNull( parameterIndex, sqlType );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setBoolean(int, boolean)
   */
  public void setBoolean( int parameterIndex, boolean x ) throws SQLException
    {
    this.preparedStatement.setBoolean( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setBoolean( String parameterName, boolean x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setBoolean( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setByte(int, byte)
   */
  public void setByte( int parameterIndex, byte x ) throws SQLException
    {
    this.preparedStatement.setByte( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setByte( String parameterName, byte x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setByte( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setShort(int, short)
   */
  public void setShort( int parameterIndex, short x ) throws SQLException
    {
    this.preparedStatement.setShort( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setShort( String parameterName, short x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setShort( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setInt(int, int)
   */
  public void setInt( int parameterIndex, int x ) throws SQLException
    {
    // TODO Auto-generated method stub
    this.preparedStatement.setInt( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setInt( String parameterName, int x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setInt( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setLong(int, long)
   */
  public void setLong( int parameterIndex, long x ) throws SQLException
    {
    this.preparedStatement.setLong( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setLong( String parameterName, long x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setLong( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setFloat(int, float)
   */
  public void setFloat( int parameterIndex, float x ) throws SQLException
    {
    this.preparedStatement.setFloat( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setFloat( String parameterName, float x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setFloat( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setDouble(int, double)
   */
  public void setDouble( int parameterIndex, double x ) throws SQLException
    {
    this.preparedStatement.setDouble( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setDouble( String parameterName, double x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setDouble( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
   */
  public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException
    {
    this.preparedStatement.setBigDecimal( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setBigDecimal( String parameterName, BigDecimal x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setBigDecimal( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setString(int, java.lang.String)
   */
  public void setString( int parameterIndex, String x ) throws SQLException
    {
    this.preparedStatement.setString( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setString( String parameterName, String x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setString( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setBytes(int, byte[])
   */
  public void setBytes( int parameterIndex, byte[] x ) throws SQLException
    {
    this.preparedStatement.setBytes( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setBytes( String parameterName, byte[] x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setBytes( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
   */
  public void setDate( int parameterIndex, Date x ) throws SQLException
    {
    this.preparedStatement.setDate( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setDate( String parameterName, Date x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setDate( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
   */
  public void setTime( int parameterIndex, Time x ) throws SQLException
    {
    this.preparedStatement.setTime( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setTime( String parameterName, Time x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setTime( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
   */
  public void setTimestamp( int parameterIndex, Timestamp x ) throws SQLException
    {
    this.preparedStatement.setTimestamp( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setTimestamp( String parameterName, Timestamp x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setTimestamp( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
   */
  public void setAsciiStream( int parameterIndex, InputStream x, int length ) throws SQLException
    {
    this.preparedStatement.setAsciiStream( parameterIndex, x, length );
    }

  /**
   * @param parameterName
   * @param x
   * @param length
   * @throws SQLException
   */
  public void setAsciiStream( String parameterName, InputStream x, int length ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setAsciiStream( parameterIndex, x, length );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
   */
  public void setUnicodeStream( int parameterIndex, InputStream x, int length ) throws SQLException
    {
    this.preparedStatement.setUnicodeStream( parameterIndex, x, length );
    }

  /**
   * @param parameterName
   * @param x
   * @param length
   * @throws SQLException
   * @deprecated
   */
  public void setUnicodeStream( String parameterName, InputStream x, int length ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setUnicodeStream( parameterIndex, x, length );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
   */
  public void setBinaryStream( int parameterIndex, InputStream x, int length ) throws SQLException
    {
    this.preparedStatement.setBinaryStream( parameterIndex, x, length );
    }

  /**
   * @param parameterName
   * @param x
   * @param length
   * @throws SQLException
   */
  public void setBinaryStream( String parameterName, InputStream x, int length ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setBinaryStream( parameterIndex, x, length );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#clearParameters()
   */
  public void clearParameters( ) throws SQLException
    {
    this.preparedStatement.clearParameters( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
   */
  public void setObject( int parameterIndex, Object x, int targetSqlType, int scale ) throws SQLException
    {
    this.preparedStatement.setObject( parameterIndex, x, targetSqlType, scale );
    }

  /**
   * @param parameterName
   * @param x
   * @param targetSqlType
   * @param scale
   * @throws SQLException
   */
  public void setObject( String parameterName, Object x, int targetSqlType, int scale ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setObject( parameterIndex, x, targetSqlType, scale );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
   */
  public void setObject( int parameterIndex, Object x, int targetSqlType ) throws SQLException
    {
    this.preparedStatement.setObject( parameterIndex, x, targetSqlType );
    }

  /**
   * @param parameterName
   * @param x
   * @param targetSqlType
   * @throws SQLException
   */
  public void setObject( String parameterName, Object x, int targetSqlType ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setObject( parameterIndex, x, targetSqlType );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
   */
  public void setObject( int parameterIndex, Object x ) throws SQLException
    {
    this.preparedStatement.setObject( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setObject( String parameterName, Object x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setObject( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#execute()
   */
  public boolean execute( ) throws SQLException
    {
    return this.preparedStatement.execute( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#addBatch()
   */
  public void addBatch( ) throws SQLException
    {
    this.preparedStatement.addBatch( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
   */
  public void setCharacterStream( int parameterIndex, Reader x, int length ) throws SQLException
    {
    this.preparedStatement.setCharacterStream( parameterIndex, x, length );
    }

  /**
   * @param parameterName
   * @param x
   * @param length
   * @throws SQLException
   */
  public void setCharacterStream( String parameterName, Reader x, int length ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setCharacterStream( parameterIndex, x, length );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
   */
  public void setRef( int parameterIndex, Ref x ) throws SQLException
    {
    this.preparedStatement.setRef( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setRef( String parameterName, Ref x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setRef( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
   */
  public void setBlob( int parameterIndex, Blob x ) throws SQLException
    {
    this.preparedStatement.setBlob( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setBlob( String parameterName, Blob x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setBlob( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
   */
  public void setClob( int parameterIndex, Clob x ) throws SQLException
    {
    this.preparedStatement.setClob( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setClob( String parameterName, Clob x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setClob( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
   */
  public void setArray( int parameterIndex, Array x ) throws SQLException
    {
    this.preparedStatement.setArray( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setArray( String parameterName, Array x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setArray( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#getMetaData()
   */
  public ResultSetMetaData getMetaData( ) throws SQLException
    {
    return this.preparedStatement.getMetaData( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)
   */
  public void setDate( int parameterIndex, Date x, Calendar cal ) throws SQLException
    {
    this.preparedStatement.setDate( parameterIndex, x, cal );
    }

  /**
   * @param parameterName
   * @param x
   * @param cal
   * @throws SQLException
   */
  public void setDate( String parameterName, Date x, Calendar cal ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setDate( parameterIndex, x, cal );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)
   */
  public void setTime( int parameterIndex, Time x, Calendar cal ) throws SQLException
    {
    this.preparedStatement.setTime( parameterIndex, x, cal );
    }

  /**
   * @param parameterName
   * @param x
   * @param cal
   * @throws SQLException
   */
  public void setTime( String parameterName, Time x, Calendar cal ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setTime( parameterIndex, x, cal );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
   */
  public void setTimestamp( int parameterIndex, Timestamp x, Calendar cal ) throws SQLException
    {
    this.preparedStatement.setTimestamp( parameterIndex, x, cal );
    }

  /**
   * @param parameterName
   * @param x
   * @param cal
   * @throws SQLException
   */
  public void setTimestamp( String parameterName, Timestamp x, Calendar cal ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setTimestamp( parameterIndex, x, cal );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
   */
  public void setNull( int parameterIndex, int sqlType, String typeName ) throws SQLException
    {
    this.preparedStatement.setNull( parameterIndex, sqlType, typeName );
    }

  /**
   * @param parameterName
   * @param sqlType
   * @param typeName
   * @throws SQLException
   */
  public void setNull( String parameterName, int sqlType, String typeName ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setNull( parameterIndex, sqlType, typeName );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
   */
  public void setURL( int parameterIndex, URL x ) throws SQLException
    {
    this.preparedStatement.setURL( parameterIndex, x );
    }

  /**
   * @param parameterName
   * @param x
   * @throws SQLException
   */
  public void setURL( String parameterName, URL x ) throws SQLException
    {
    for( int parameterIndex : this.parameterMapping.get( parameterName ) )
      {
      this.preparedStatement.setURL( parameterIndex, x );
      }
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.PreparedStatement#getParameterMetaData()
   */
  public ParameterMetaData getParameterMetaData( ) throws SQLException
    {
    return this.preparedStatement.getParameterMetaData( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeQuery(java.lang.String)
   */
  public ResultSet executeQuery( String sql ) throws SQLException
    {
    return this.preparedStatement.executeQuery( sql );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String)
   */
  public int executeUpdate( String sql ) throws SQLException
    {
    return this.preparedStatement.executeUpdate( sql );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#close()
   */
  public void close( ) throws SQLException
    {
    this.preparedStatement.close( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMaxFieldSize()
   */
  public int getMaxFieldSize( ) throws SQLException
    {
    return this.preparedStatement.getMaxFieldSize( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setMaxFieldSize(int)
   */
  public void setMaxFieldSize( int max ) throws SQLException
    {
    this.preparedStatement.setMaxFieldSize( max );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMaxRows()
   */
  public int getMaxRows( ) throws SQLException
    {
    return this.preparedStatement.getMaxRows( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setMaxRows(int)
   */
  public void setMaxRows( int max ) throws SQLException
    {
    this.preparedStatement.setMaxRows( max );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */
  public void setEscapeProcessing( boolean enable ) throws SQLException
    {
    this.preparedStatement.setEscapeProcessing( enable );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getQueryTimeout()
   */
  public int getQueryTimeout( ) throws SQLException
    {
    return this.preparedStatement.getQueryTimeout( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setQueryTimeout(int)
   */
  public void setQueryTimeout( int seconds ) throws SQLException
    {
    this.preparedStatement.setQueryTimeout( seconds );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#cancel()
   */
  public void cancel( ) throws SQLException
    {
    this.preparedStatement.cancel( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getWarnings()
   */
  public SQLWarning getWarnings( ) throws SQLException
    {
    return this.preparedStatement.getWarnings( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#clearWarnings()
   */
  public void clearWarnings( ) throws SQLException
    {
    this.preparedStatement.clearWarnings( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setCursorName(java.lang.String)
   */
  public void setCursorName( String name ) throws SQLException
    {
    this.preparedStatement.setCursorName( name );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String)
   */
  public boolean execute( String sql ) throws SQLException
    {
    return this.preparedStatement.execute( sql );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSet()
   */
  public ResultSet getResultSet( ) throws SQLException
    {
    return this.preparedStatement.getResultSet( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getUpdateCount()
   */
  public int getUpdateCount( ) throws SQLException
    {
    return this.preparedStatement.getUpdateCount( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMoreResults()
   */
  public boolean getMoreResults( ) throws SQLException
    {
    return this.preparedStatement.getMoreResults( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setFetchDirection(int)
   */
  public void setFetchDirection( int direction ) throws SQLException
    {
    this.preparedStatement.setFetchDirection( direction );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getFetchDirection()
   */
  public int getFetchDirection( ) throws SQLException
    {
    return this.preparedStatement.getFetchDirection( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#setFetchSize(int)
   */
  public void setFetchSize( int rows ) throws SQLException
    {
    this.preparedStatement.setFetchSize( rows );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getFetchSize()
   */
  public int getFetchSize( ) throws SQLException
    {
    return this.preparedStatement.getFetchSize( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSetConcurrency()
   */
  public int getResultSetConcurrency( ) throws SQLException
    {
    return this.preparedStatement.getResultSetConcurrency( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSetType()
   */
  public int getResultSetType( ) throws SQLException
    {
    return this.preparedStatement.getResultSetType( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#addBatch(java.lang.String)
   */
  public void addBatch( String sql ) throws SQLException
    {
    this.preparedStatement.addBatch( sql );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#clearBatch()
   */
  public void clearBatch( ) throws SQLException
    {
    this.preparedStatement.clearBatch( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeBatch()
   */
  public int[] executeBatch( ) throws SQLException
    {
    return this.preparedStatement.executeBatch( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getConnection()
   */
  public Connection getConnection( ) throws SQLException
    {
    return this.preparedStatement.getConnection( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getMoreResults(int)
   */
  public boolean getMoreResults( int current ) throws SQLException
    {
    return this.preparedStatement.getMoreResults( current );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getGeneratedKeys()
   */
  public ResultSet getGeneratedKeys( ) throws SQLException
    {
    return this.preparedStatement.getGeneratedKeys( );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String, int)
   */
  public int executeUpdate( String sql, int autoGeneratedKeys ) throws SQLException
    {
    return this.preparedStatement.executeUpdate( sql, autoGeneratedKeys );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
   */
  public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException
    {
    return this.preparedStatement.executeUpdate( sql, columnIndexes );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
   */
  public int executeUpdate( String sql, String[] columnNames ) throws SQLException
    {
    return this.preparedStatement.executeUpdate( sql, columnNames );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String, int)
   */
  public boolean execute( String sql, int autoGeneratedKeys ) throws SQLException
    {
    return this.preparedStatement.execute( sql, autoGeneratedKeys );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String, int[])
   */
  public boolean execute( String sql, int[] columnIndexes ) throws SQLException
    {
    return this.preparedStatement.execute( sql, columnIndexes );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
   */
  public boolean execute( String sql, String[] columnNames ) throws SQLException
    {
    return this.preparedStatement.execute( sql, columnNames );
    }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Statement#getResultSetHoldability()
   */
  public int getResultSetHoldability( ) throws SQLException
    {
    return this.preparedStatement.getResultSetHoldability( );
    }

  /**
   * @param connection
   * @param sql
   * @return
   * @throws SQLException
   */
  public static BindablePreparedStatement createBindablePreparedStatement( Connection connection, String sql ) throws SQLException
    {
    StringBuilder parsedSql = new StringBuilder( sql );
    Pattern namedParameterPattern = Pattern.compile( ":[a-zA-Z][a-zA-Z_0-9-]*" );
    Matcher namedParamaterMatcher = namedParameterPattern.matcher( sql );

    Map<String, List<Integer>> parameterMapping = new HashMap<String, List<Integer>>( );
    int currentParameterIndex = 1;

    while( namedParamaterMatcher.find( ) )
      {
      String parameterName = namedParamaterMatcher.group( ).substring( 1 );
      if( !parameterMapping.containsKey( parameterName ) )
        {
        parameterMapping.put( parameterName, new LinkedList<Integer>( ) );
        }
      parameterMapping.get( parameterName ).add( currentParameterIndex );
      currentParameterIndex++;

      parsedSql.replace( namedParamaterMatcher.start( ), namedParamaterMatcher.end( ), "?" );
      namedParamaterMatcher.reset( parsedSql );
      }

    PreparedStatement preparedStatement = connection.prepareStatement( parsedSql.toString( ) );
    return new BindablePreparedStatement( preparedStatement, parameterMapping );
    }
  }
