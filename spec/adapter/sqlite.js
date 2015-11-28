import co from 'co';
import sqlite3 from 'sqlite3';
import { extend, merge } from 'extend-merge';
import { Database } from '../../src';
import { Sqlite as SqliteDialect } from 'sql-dialect';

/**
 * MySQL adapter
 */
class Sqlite extends Database {
  /**
   * Check for required PHP extension, or supported database feature.
   *
   * @param  String  feature Test for support for a specific feature, i.e. `"transactions"`
   *                          or `"arrays"`.
   * @return Boolean          Returns `true` if the particular feature (or if MySQL) support
   *                          is enabled, otherwise `false`.
   */
  static enabled(feature) {
    var features = {
      arrays: false,
      transactions: true,
      booleans: true,
      default: false
    };
    if (!arguments.length) {
      return extend({}, features);
    }
    return features[feature];
  }

  /**
   * Constructs the MySQL adapter and sets the default port to 3306.
   *
   * @param Object config Configuration options for this class. Available options
   *                      defined by this class:
   *                      - `'host'`: _string_ The IP or machine name where MySQL is running,
   *                                  followed by a colon, followed by a port number or socket.
   *                                  Defaults to `'localhost'`.
   */
  constructor(config) {
    var defaults = {
      classes: {
        dialect: SqliteDialect
      },
      database: undefined,
      mode : sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
      connect: true,
      alias: true,
      driver: undefined,
      dialect: true
    };
    config = merge({}, defaults, config);
    super(config);

    /**
     * Specific value denoting whether or not table aliases should be used in DELETE and UPDATE queries.
     *
     * @var Boolean
     */
    this._alias = config.alias;

    /**
     * Stores a connection to a remote resource.
     *
     * @var Function
     */
    this._driver = config.driver;

    /**
     * The SQL dialect instance.
     *
     * @var Function
     */
    var dialect = this.classes().dialect;

    if (typeof this._dialect !== 'object') {
      this._dialect = new dialect({
        caster: function(value, states) {
          var type = states && states.type ? states.type : this.constructor.getType(value);
          if (typeof type === 'function') {
            type = type(states.name);
          }
          return this.format('datasource', type, value);
        }.bind(this)
      });
    }

    if (this._config.connect) {
      this.connect();
    }
  }

  /**
   * Returns the pdo connection instance.
   *
   * @return Function
   */
  driver() {
    return this._driver;
  }

  /**
   * Connects to the database using the options provided to the class constructor.
   *
   * @return boolean Returns `true` if a database connection could be established,
   *                 otherwise `false`.
   */
  connect() {
    if (this._driver) {
      return this._driver;
    }

    var config = this.config();

    if (!config.database) {
      throw new Error('Error, no database name has been configured.');
    }

    return new Promise(function(accept, reject) {
      this._driver = new sqlite3.Database(config.database, config.mode, function(err) {
        if (err) {
          reject(new Error('Unable to connect to host , error ' + err.code + ' ' + err.stack));
        }
        accept(this._driver)
      }.bind(this));
    }.bind(this));
  }

  /**
   * Checks the connection status of this data source.
   *
   * @return Boolean Returns a boolean indicating whether or not the connection is currently active.
   *                 This value may not always be accurate, as the connection could have timed out or
   *                 otherwise been dropped by the remote resource during the course of the request.
   */
  connected() {
    return !!this._driver;
  }

  /**
   * Finds records using a SQL query.
   *
   * @param  string sql  SQL query to execute.
   * @param  array  data Array of bound parameters to use as values for query.
   * @return object       A `Cursor` instance.
   */
  query(sql, data, options) {
    var self = this;
    return new Promise(function(accept, reject) {
      var defaults = {};
      options = extend({}, defaults, options);

      var cursor = self.constructor.classes().cursor;

      var response = function(err, data) {
        if (err) {
          return reject(err);
        }
        if (this !== undefined) {
          self._lastInsertId = this.lastID;
        }
        accept(data ? new cursor({ data: data }) : data);
      };

      if (sql.match(/SELECT/i)) {
        self.driver().all(sql, response);
      } else {
        self.driver().run(sql, response);
      }
    });
  }

  /**
   * Returns the last insert id from the database.
   *
   * @return mixed Returns the last insert id.
   */
  lastInsertId() {
    return this._lastInsertId;
  }

  /**
   * Returns the list of tables in the currently-connected database.
   *
   * @return Object Returns an object of sources to which models can connect.
   */
  sources() {
    var select = this.dialect().statement('select');
    select.fields('name')
      .from('sqlite_master')
      .where({ type: 'table' });
    return this._sources(select);
  }

  /**
   * Gets the column schema for a given MySQL table.
   *
   * @param  mixed    name   Specifies the table name for which the schema should be returned.
   * @param  Object   fields Any schema data pre-defined by the model.
   * @param  Object   meta
   * @return Function        Returns a shema definition.
   */
  describe(name, fields, meta) {
    return co(function*() {
      if (arguments.length === 1) {
        fields = yield this.fields(name);
      }

      var schema = this.classes().schema;

      return new schema({
        connection: this,
        source: name,
        fields: fields,
        meta: meta
      });
    }.bind(this));
  }

  /**
   * Extracts fields definitions of a table.
   *
   * @param  String name The table name.
   * @return Object      The fields definitions.
   */
  fields(name) {
    return co(function*() {
      var fields = {};
      var columns = yield this.query('PRAGMA table_info(' + name +')');

      for (var key in columns) {
        var column = columns[key];
        var field = this._column(column.type);
        var dft = column.dflt_value;

        switch (field.type) {
          case 'boolean':
            if (dft === '1') {
              dft = true;
            }
            if (dft === '0') {
              dft = false;
            }
            break;
          case 'timestamp':
            dft = dft !== 'CURRENT_TIMESTAMP' ? dft : null;
            break;
        }

        fields[column.name] = extend({}, {
          null: (column.notnull === '1' ? false : true),
          'default': dft
        }, field);
      }
      return fields;
    });
  }

  /**
   * Converts database-layer column types to basic types.
   *
   * @param  string real Real database-layer column type (i.e. `"varchar(255)"`)
   * @return array        Column type (i.e. "string") plus 'length' when appropriate.
   */
  _column(real) {
    var matches = real.match(/(?\w+)(?:\((?[\d,]+)\))?/);
    var column = {};
    column.type = matches[1];
    column.length = matches[2];
    column.use = column.type;

    if (column.length) {
      var length = column.length.split(',');
      column.length = Number.parseInt(length[0]);
      if (length[1]) {
        column.precision = Number.parseInt(length[1]);
      }
    }

    column.type = this.dialect().mapped(column);
    return column;
  }

  /**
   * Disconnects the adapter from the database.
   *
   * @return Boolean Returns `true` on success, else `false`.
   */
  disconnect() {
    this.driver().close();
    this._driver = undefined;
    return true;
  }
}

export default Sqlite;
