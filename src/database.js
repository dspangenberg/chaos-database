var co = require('co');
var dateFormat = require('dateformat');
var extend = require('extend-merge').extend;
var merge = require('extend-merge').merge;
var Dialect = require('sql-dialect').Dialect;
var Source = require('chaos-orm').Source;
var Cursor = require('./cursor');
var Schema = require('./schema');

/**
 * An adapter base class for SQL based driver
 */
class Database extends Source {
  /**
   * Gets/sets class dependencies.
   *
   * @param  Object classes The classes dependencies to set or none to get them.
   * @return Object         The classes dependencies.
   */
  static classes(classes) {
    if (arguments.length) {
      this._classes = extend({}, this._classes, classes);
    }
    return this._classes;
  }

  /**
   * Creates the database object and set default values for it.
   *
   * Options defined:
   *  - `'dns'`       : _string_ The full dsn connection url. Defaults to `null`.
   *  - `'database'`  : _string_ Name of the database to use. Defaults to `null`.
   *  - `'host'`      : _string_ Name/address of server to connect to. Defaults to 'localhost'.
   *  - `'username'`  : _string_ Username to use when connecting to server. Defaults to 'root'.
   *  - `'password'`  : _string_ Password to use when connecting to server. Defaults to `''`.
   *  - `'persistent'`: _boolean_ If true a persistent connection will be attempted, provided the
   *                    adapter supports it. Defaults to `true`.
   *  - `'dialect'`   : _object_ A SQL dialect adapter
   *
   * @param  Object config Configuration options.
   * @return Database object.
   */
  constructor(config) {
    super(config);

    var defaults = {
      classes: {},
      host: 'localhost',
      username: undefined,
      password: undefined,
      database: undefined,
      options: {},
      dialect: undefined,
      meta: { key: 'id', locked: true }
    };
    config = extend({}, defaults, config);

    /**
     * Default entity and set classes used by subclasses of `Source`.
     *
     * @var Object
     */
    this._classes = extend({}, this.constructor._classes, config.classes);

    /**
     * The last inserted record ID.
     *
     * @var mixed
     */
    this._lastInsertId = undefined;

    /**
     * The transaction level
     *
     */
    this._transactionLevel = 0;

    /**
     * Stores configuration information for object instances at time of construction.
     *
     * @var Object
     */
    this._config = extend({}, config);
    delete this._config.classes;
    delete this._config.dialect;

    /**
     * The SQL dialect instance.
     *
     * @var Function
     */
    this.dialect(config.dialect);

    if (!this._dialect) {
      this._initDialect();
    }

    var handlers = this._handlers;

    this.formatter('datasource', 'id',        handlers.datasource['string']);
    this.formatter('datasource', 'serial',    handlers.datasource['string']);
    this.formatter('datasource', 'integer',   handlers.datasource['string']);
    this.formatter('datasource', 'float',     handlers.datasource['string']);
    this.formatter('datasource', 'decimal',   handlers.datasource['decimal']);
    this.formatter('datasource', 'date',      handlers.datasource['date']);
    this.formatter('datasource', 'datetime',  handlers.datasource['datetime']);
    this.formatter('datasource', 'boolean',   handlers.datasource['boolean']);
    this.formatter('datasource', 'null',      handlers.datasource['null']);
    this.formatter('datasource', 'string',    handlers.datasource['quote']);
    this.formatter('datasource', 'json',      handlers.datasource['json']);
    this.formatter('datasource', '_default_', handlers.datasource['quote']);

    this.formatter('array', 'id',     handlers.array['integer']);
    this.formatter('array', 'serial', handlers.array['integer']);
    this.formatter('cast', 'id',      handlers.cast['integer']);
    this.formatter('cast', 'serial',  handlers.cast['integer']);
  }

  /**
   * Gets/sets instance dependencies.
   *
   * @param  Object classes The classes dependencies to set or nothing to get the defined ones.
   * @return Object         The classes dependencies.
   */
  classes(classes) {
    if (arguments.length) {
      this._classes = extend({}, this._classes, classes);
    }
    return this._classes;
  }

  /**
   * Return the source configuration.
   *
   * @return Object.
   */
  config() {
    return this._config;
  }

  /**
   * Get/set the SQL dialect instance.
   *
   * @param  Object dialect The dialect instance to set or none to get the setted one.
   * @return Object.
   */
  dialect(dialect) {
    if (arguments.length) {
      this._dialect = dialect;
      return this;
    }
    return this._dialect;
  }

  /**
   * Initialize a new dialect instance.
   */
  _initDialect() {
    var Dialect = this.classes().dialect;
    var dialect = new Dialect({
      quote: function(string) {
        return dialect.quote(String(string));
      },
      caster: function(value, states) {
        if (states & states.schema) {
          return states.schema.format('datasource', states.name, value);
        }
        return this.convert('datasource', this.constructor.getType(value), value);
      }
    });
    this._dialect = dialect;
  }

  /**
   * Returns the last inserted record ID from the database.
   *
   * @return mixed The last inserted record ID.
   */
  lastInsertId() {
    return this._lastInsertId;
  }

  /**
   * Start a new database transaction.
   *
   * return Promise
   * @throws Error
   */
  beginTransaction() {
    return co(function*() {
      if (this._transactionLevel === 0) {
        yield this.openTransaction();
      } else if (this._transactionLevel >0 && this.constructor.enabled('savepoints')) {
        var name = 'TRANS' + (this._transactionLevel + 1);
        yield this.execute("SAVEPOINT " + name);
      }
      this._transactionLevel++;
    }.bind(this));
  }

  /**
   * Execute a Closure within a transaction.
   *
   * @param  Closure transaction
   * @param  Integer maxRepeat
   * @return Promise
   *
   * @throws Error
   */
  transaction(transaction, maxRepeat) {
    return co(function*() {
      maxRepeat = maxRepeat || 1;
      for (var count = 1; count <= maxRepeat; count++) {
        yield this.beginTransaction();
        try {
          yield transaction(this);
        } catch (exception) {
          yield this.rollback();
          throw exception;
        }
        try {
          yield this.commit();
        } catch (exception) {
          this._transactionException(exception, count, maxRepeat);
        }
      }
    }.bind(this));
  }

  /**
   * Get the number of active transactions.
   *
   * @return Integer
   */
  transactionLevel() {
    return this._transactionLevel;
  }

  /**
   * Commit the active database transaction.
   */
  commit() {
    return co(function*() {
      if (this._transactionLevel > 0) {
        this._transactionLevel--;
      }
      if (this._transactionLevel === 0) {
        yield this.execute("COMMIT");
      }
    }.bind(this));
  }

  /**
   * Rollback the active database transaction.
   *
   * @param Integer|none toLevel
   */
  rollback(toLevel) {
    return co(function*() {
      toLevel = !arguments.length ? this._transactionLevel - 1 : toLevel;
      if (toLevel < 0 || toLevel >= this._transactionLevel) {
        return;
      }
      if (toLevel === 0) {
        yield this.execute("ROLLBACK");
      } else if (this.constructor.enabled('savepoints')) {
        var name = 'TRANS' + (toLevel + 1);
        yield this.execute("ROLLBACK TO SAVEPOINT " + name);
      }
      this._transactionLevel = toLevel;
    }.bind(this));
  }

  /**
   * Gets the column schema for a given MySQL table.
   *
   * @param  mixed    name    Specifies the table name for which the schema should be returned.
   * @param  Object   columns Any schema columns pre-defined by the model.
   * @param  Object   meta
   * @return Function         Returns a shema definition.
   */
  describe(name, columns, meta) {
    var nbargs = arguments.length;
    return co(function*() {
      if (nbargs === 1) {
        columns = yield this.fields(name);
      }

      var schema = this.classes().schema;

      return new schema({
        connection: this,
        source: name,
        columns: columns,
        meta: meta
      });
    }.bind(this));
  }

  /**
   * Returns the list of tables in the currently-connected database.
   *
   * @return array Returns an array of sources to which models can connect.
   */
  _sources(sql) {
    return co(function*() {
      var result = yield this.query(sql.toString());

      var sources = {};
      for(var source of result) {
        var key = Object.keys(source);
        var name = source[key];
        sources[name] = name;
      }
      return sources;
    }.bind(this));
  }

  /**
   * Formats a value according to its definition.
   *
   * @param  String mode  The format mode (i.e. `'cast'` or `'datasource'`).
   * @param  String type  The type name.
   * @param  mixed  value The value to format.
   * @return mixed        The formated value.
   */
  convert(mode, type, value, options) {
    if (value !== null && typeof value === 'object' && value.constructor === Object) {
      var key = Object.keys(value)[0];
      var dialect = this.dialect();
      if (dialect && dialect.isOperator(key)) {
        return dialect.format(key, value[key]);
      }
    }
    return super.convert(mode, type, value, options);
  }

  /**
   * Returns default casting handlers.
   *
   * @return Object
   */
  _handlers() {
    return merge({}, super._handlers(), {
      datasource: {
        'decimal': function(value, options) {
          var defaults = { precision: 2 };
          options = extend({}, defaults, options);
          return Number(value).toFixed(options.precision);
        },
        'quote': function(value, options) {
          return this.dialect().quote(String(value));
        }.bind(this),
        'date': function(value, options) {
          options = options || {};
          options.format = options.format ? options.format : 'yyyy-mm-dd';
          return this.convert('datasource', 'datetime', value, options);
        }.bind(this),
        'datetime': function(value, options) {
          options = options || {};
          options.format = options.format ? options.format : 'yyyy-mm-dd HH:MM:ss';
          if (Number(Number.parseInt(value)) === value) {
            value = Number.parseInt(value) * 1000;
          }
          var date = !(value instanceof Date) ? new Date(value) : value;
          if (Number.isNaN(date.getTime())) {
            throw new Error("Invalid date `" + value + "`, can't be parsed.");
          }
          return this.dialect().quote(dateFormat(date, options.format, true));
        }.bind(this),
        'boolean': function(value, options) {
          return value ? 'TRUE' : 'FALSE';
        },
        'null': function(value, options) {
          return 'NULL';
        },
        'json': function(value, options) {
          if (value && value.data) {
            value = value.data();
          }
          return this.dialect().quote(JSON.stringify(value));
        }.bind(this)
      }
    });
  }

  /**
   * Handle an exception encountered when running a transacted statement.
   *
   * @param  Error   exception
   * @param  Integer count
   * @param  Integer maxRepeat
   *
   * @throws Error
   */
  _transactionException(exception, count, maxRepeat) {
    if (this.constructor.isDeadlockException(exception)) {
      this._transactionLevel--;
      throw exception;
    }
    this.rollback();
    if (count >= maxRepeat) {
      throw exception;
    }
  }

  /**
   * Check a lost connection exception.
   *
   * @param  Error   exception
   * @return Boolean
   */
  static isLostConnectionException(exception) {
    var message = exception.message.toLowerCase();
    for (var needle of [
      'no connection to the server',                 // PDO
      'server has gone away',                        // MySQL
      'lost connection',                             // MySQL
      'resource deadlock avoided',                   // MySQL
      'Transaction() on null',                       // MySQL
      'decryption failed or bad record mac',         // PostgreSQL
      'server closed the connection unexpectedly',   // PostgreSQL
      'ssl connection has been closed unexpectedly', // PostgreSQL
      'is dead or not enabled'                       // SQL Server
    ]) {
      if (message.indexOf(needle) !== -1) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determine if the given exception was caused by a deadlock.
   *
   * @param  Error   exception
   * @return boolean
   */
  static isDeadlockException(exception) {
    var message = exception.message.toLowerCase();
    for (var needle of [
      'deadlock found when trying to get lock', // MySQL
      'deadlock detected',                      // PostgreSQL
      'has been chosen as the deadlock victim', // SQL Server
      'the database file is locked',            // SQLite
      'database is locked',                     // SQLite
      'database table is locked',               // SQLite
      'a table in the database is locked'       // SQLite
    ]) {
      if (message.indexOf(needle) !== -1) {
        return true;
      }
    }
    return false;
  }
}

/**
 * Class dependencies.
 *
 * @var array
 */
Database._classes = {
  cursor: Cursor,
  schema: Schema,
  dialect: Dialect
};

module.exports = Database;
