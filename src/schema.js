var co = require('co');
var extend = require('extend-merge').extend;
var merge = require('extend-merge').merge;
var BaseSchema = require('chaos-orm').Schema;
var Relationship = require('chaos-orm').Relationship;
var BelongsTo = require('chaos-orm').BelongsTo;
var HasOne = require('chaos-orm').HasOne;
var HasMany = require('chaos-orm').HasMany;
var HasManyThrough = require('chaos-orm').HasManyThrough;
var Query = require('./query');

class Schema extends BaseSchema {

  /**
   * Configures the meta for use.
   *
   * @param Object config Possible options are:
   *                      - `'connection'`  _Function_ : The connection instance (defaults to `undefined`).
   */
  constructor(config) {
    var defaults = {
      connection: undefined,
    };

    config = merge({}, defaults, config);
    super(config);

    /**
     * The connection instance.
     *
     * @var Object
     */
    this._connection = undefined;

    if (config.connection) {
      this.connection(config.connection);
    }
  }

  /**
   * Gets/sets the connection object to which this class is bound.
   *
   * @param  Object connection The connection instance to set or `null` to get the current one.
   * @return mixed             Returns the connection instance on get or `this` on set.
   */
  connection(connection) {
    if (arguments.length) {
      this._connection = connection;
      merge(this._formatters, this._connection.formatters());
      return this;
    }
    if (!this._connection) {
      throw new Error("Error, missing connection for this schema.");
    }
    return this._connection;
  }

  /**
   * Returns a query to retrieve data from the connected data source.
   *
   * @param  Object options Query options.
   * @return Object         An instance of `Query`.
   */
  query(options) {
    var defaults = {
      connection: this.connection(),
      model: this.reference()
    };
    options = extend({}, defaults, options);

    var query = this.constructor.classes().query;

    if (!options.model) {
      throw new Error("Missing model for this schema, can't create a query.");
    }
    return new query(options);
  }

  /**
   * Creates the schema.
   *
   * @param  Object  options An object of options.
   * @return Promise
   */
  create(options) {
    return co(function*() {
      var defaults = {
        soft: true
      };
      options = extend({}, defaults, options);

      if (this._source === undefined) {
        throw new Error("Missing table name for this schema.");
      }

      var query = this.connection().dialect().statement('create table', { schema: this });
      query.ifNotExists(options.soft)
           .table(this._source)
           .columns(this.columns(true))
           .constraints(this.meta('constraints'))
           .meta(this.meta('table'));

      yield this.connection().query(query.toString());

    }.bind(this));
  }

  /**
   * Bulk inserts
   *
   * @param  Array    inserts An array of entities to insert.
   * @param  Function filter  The filter handler for which extract entities values for the insertion.
   * @return Promise
   */
  bulkInsert(inserts, filter) {
    return co(function*() {
      if (!inserts || !inserts.length) {
        return;
      }
      for (var entity of inserts) {
        yield this.insert(filter(entity));
        var id = entity.id();
        id = id == null ? this.lastInsertId() : id;
        entity.amend({[this.key()]: id}, { exists: true });
      }
    }.bind(this));
  }

  /**
   * Bulk updates
   *
   * @param  Array    updates An array of entities to update.
   * @param  Function filter  The filter handler for which extract entities values to update.
   * @return Promise
   */
  bulkUpdate(updates, filter) {
    return co(function*() {
      if (!updates || !updates.length) {
          return;
      }
      for (var entity of updates) {
        var id = entity.id();
        if (id === undefined) {
          throw new Error("Can't update an existing entity with a missing ID.");
        }
        yield this.update(filter(entity), {[this.key()] : id});
        entity.amend();
      }
    }.bind(this));
  }

  /**
   * Inserts a records  with the given data.
   *
   * @param  Object  data       Typically an object of key/value pairs that specify the new data with which
   *                            the records will be updated. For SQL databases, this can optionally be
   *                            an SQL fragment representing the `SET` clause of an `UPDATE` query.
   * @param  Object  options    Any database-specific options to use when performing the operation.
   * @return Promise
   */
  insert(data, options) {
    var key = this.key();

    if (data[key] === undefined) {
      var constructor = this.connection().constructor;
      data[key] = constructor.enabled('default') ? { ':plain' : 'default' } : null;
    }

    var insert = this.connection().dialect().statement('insert', { schema: this });
    insert.into(this.source()).values(data);

    return this.connection().query(insert.toString());
  }

  /**
   * Updates multiple records with the given data, restricted by the given set of criteria (optional).
   *
   * @param  Object  data       Typically an array of key/value pairs that specify the new data with which
   *                            the records will be updated. For SQL databases, this can optionally be
   *                            an SQL fragment representing the `SET` clause of an `UPDATE` query.
   * @param  mixed   conditions The conditions with key/value pairs representing the scope of the records or
   *                            documents to be updated.
   * @param  Object  options    Any database-specific options to use when performing the operation.
   * @return Promise
   */
  update(data, conditions, options) {
    var update = this.connection().dialect().statement('update', { schema: this });
    update.table(this.source())
          .where(conditions)
          .values(data);

    return this.connection().query(update.toString());
  }

  /**
   * Removes multiple documents or records based on a given set of criteria. **WARNING**: If no
   * criteria are specified, or if the criteria (`conditions`) is an empty value (i.e. an empty
   * array or `null`), all the data in the backend data source (i.e. table or collection) _will_
   * be deleted.
   *
   * @param  mixed    conditions The conditions with key/value pairs representing the scope of the records or
   *                             documents to be deleted.
   * @return Promise
   */
  truncate(conditions) {
    return co(function*() {
      var del = this.connection().dialect().statement('delete');

      del.from(this.source())
         .where(conditions);

      return this.connection().query(del.toString());
    }.bind(this));
  }

  /**
   * Drops the schema
   *
   * @param  Object  options An array of options.
   * @return Boolean         Returns `true` if insert operations succeeded, `false` otherwise.
   */
  drop(options) {
    return co(function*() {
      var defaults = {
        soft: true,
        cascade: false,
        restrict: false
      };
      options = extend({}, defaults, options);

      if (this._source === undefined) {
        throw new Error("Missing table name for this schema.");
      }

      var query = this.connection().dialect().statement('drop table');
      query.ifExists(options.soft)
          .table(this._source)
          .cascade(options.cascade)
          .restrict(options.restrict);

      yield this.connection().query(query.toString());

    }.bind(this));
  }

  /**
   * Returns the last insert id from the database.
   *
   * @return mixed Returns the last insert id.
   */
  lastInsertId() {
    var sequence = this.source() + '_' + this.key() + '_seq';
    return this.connection().lastInsertId(sequence);
  }

  /**
   * Formats a value according to its type.
   *
   * @param   String mode    The format mode (i.e. `'cast'` or `'datasource'`).
   * @param   String type    The format type.
   * @param   mixed  value   The value to format.
   * @param   mixed  options The options array to pass the the formatter handler.
   * @return  mixed          The formated value.
   */
  convert(mode, type, value, options) {
    var formatter;
    type = value === null ? 'null' : type;
    if (value !== null && typeof value === 'object' && value.constructor === Object) {
      var key = Object.keys(value)[0];
      var connection = this._connection;
      if (connection && connection.dialect().isOperator(key)) {
        return connection.dialect().format(key, value[key]);
      }
    }
    if (this._formatters[mode] && this._formatters[mode][type]) {
      formatter = this._formatters[mode][type];
    } else if (this._formatters[mode] && this._formatters[mode]._default_) {
      formatter = this._formatters[mode]._default_;
    }
    return formatter ? formatter(value, options) : value;
  }
}

/**
 * Class dependencies.
 *
 * @var array
 */
Schema._classes = {
  relationship: Relationship,
  belongsTo: BelongsTo,
  hasOne: HasOne,
  hasMany: HasMany,
  hasManyThrough: HasManyThrough,
  query: Query
};

module.exports = Schema;
