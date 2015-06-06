package com.datatorrent.lib.db.jdbc;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import java.sql.*;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTransactionablePOJOOutputOperator extends AbstractJdbcTransactionableOutputOperator<Object>
{
  @NotNull
  private ArrayList<String> dataColumns;
  //These are extracted from table metadata
  private ArrayList<Integer> columnDataTypes;

  public ArrayList<String> getDataColumns()
  {
    return dataColumns;
  }

  public void setDataColumns(ArrayList<String> dataColumns)
  {
    this.dataColumns = dataColumns;
  }
  @NotNull
  private String tablename;



  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }
  @NotNull
  private ArrayList<String> expressions;
  private transient ArrayList<Object> getters;
  private String insertStatement;



  @Override
  public void setup(OperatorContext context)
  {
    StringBuilder columns = new StringBuilder("");
    StringBuilder values = new StringBuilder("");
    for (int i = 0; i < dataColumns.size(); i++) {
      columns.append(dataColumns.get(i));
      values.append("?");
      if (i < dataColumns.size() - 1) {
        columns.append(",");
        values.append(",");
      }
    }
    insertStatement = "INSERT INTO "
            + tablename
            + " (" + columns.toString() + ")"
            + " VALUES (" + values.toString() + ")";
    super.setup(context);
    Connection conn = store.getConnection();
    LOG.debug("Got Connection.");
    try {
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery("select * from " + tablename);

      ResultSetMetaData rsMetaData = rs.getMetaData();

      int numberOfColumns = 0;

      numberOfColumns = rsMetaData.getColumnCount();

      LOG.debug("resultSet MetaData column Count=" + numberOfColumns);

      for (int i = 1; i <= numberOfColumns; i++) {
        // get the designated column's SQL type.
        int type = rsMetaData.getColumnType(i);
        columnDataTypes.add(type);
        LOG.debug("sql column type is " + type);
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

  }

  public JdbcTransactionablePOJOOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<Integer>();
    getters = new ArrayList<Object>();
  }

  @Override
  public void processTuple(Object tuple)
  {
    processFirstTuple(tuple);
    super.processTuple(tuple);
  }

  public void processFirstTuple(Object tuple)
  {
    final Class<?> fqcn = tuple.getClass();
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      final String getterExpression = expressions.get(i);
      final Object getter;
      switch (type) {
        case Types.CHAR:
          getter = PojoUtils.createGetterChar(fqcn, getterExpression);
          break;
        case Types.VARCHAR:
          getter = PojoUtils.createGetter(fqcn, getterExpression, String.class);
          break;
        case Types.BOOLEAN:
        case Types.TINYINT:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpression);
          break;
        case Types.SMALLINT:
          getter = PojoUtils.createGetterShort(fqcn, getterExpression);
          break;
        case Types.INTEGER:
          getter = PojoUtils.createGetterInt(fqcn, getterExpression);
          break;
        case Types.BIGINT:
          getter = PojoUtils.createGetterLong(fqcn, getterExpression);
          break;
        case Types.FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpression);
          break;
        case Types.DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpression);
          break;
        default:
          /*
            Types.DECIMAL
            Types.DATE
            Types.TIME
            Types.ARRAY
            Types.OTHER
           */
          getter = PojoUtils.createGetter(fqcn, getterExpression, Object.class);
          break;
      }
      getters.add(getter);
    }

  }

  @Override
  protected String getUpdateCommand()
  {
    LOG.debug("insertstatement is {}", insertStatement);
    return insertStatement;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; ) {
      final int type = columnDataTypes.get(i);
      switch (type) {
        case (Types.CHAR):
          // TODO: verify that memsql driver handles char as int
          statement.setInt(++i, ((GetterChar<Object>) getters.get(i)).get(tuple));
          break;
        case (Types.VARCHAR):
          statement.setString(++i, ((Getter<Object, String>) getters.get(i)).get(tuple));
          break;
        case (Types.BOOLEAN):
          statement.setBoolean(++i, ((GetterBoolean<Object>) getters.get(i)).get(tuple));
          break;
        case (Types.SMALLINT):
          statement.setShort(++i, ((GetterShort<Object>) getters.get(i)).get(tuple));
          break;
        case (Types.INTEGER):
          statement.setInt(++i, ((GetterInt<Object>) getters.get(i)).get(tuple));
          break;
        case (Types.BIGINT):
          statement.setLong (++i, ((GetterLong<Object>) getters.get(i)).get(tuple));
          break;
        case (Types.FLOAT):
          statement.setFloat(++i, ((GetterFloat<Object>) getters.get(i)).get(tuple));
          break;
        case (Types.DOUBLE):
          statement.setDouble(++i, ((GetterDouble<Object>) getters.get(i)).get(tuple));
          break;
        default:
          /*
            Types.DECIMAL
            Types.DATE
            Types.TIME
            Types.ARRAY
            Types.OTHER
           */
          statement.setObject(++i, ((Getter<Object, Object>)getters.get(i)).get(tuple));
          break;
      }
    }
  }
    private static transient final Logger LOG = LoggerFactory.getLogger(JdbcTransactionablePOJOOutputOperator.class);

}
