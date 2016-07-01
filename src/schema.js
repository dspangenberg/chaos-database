import co from 'co';
import { extend, merge } from 'extend-merge';
import { Schema as BaseSchema, Relationship, BelongsTo, HasOne, HasMany, HasManyThrough } from 'chaos-orm';
import Query from './query';

function arrayDiff(a, b) {
  var len = a.length;
  var arr = [];

  for (var i = 0; i < len; i++) {
    if (b.indexOf(a[i]) === -1) {
      arr.push(a[i]);
    }
  }
  return arr;
}

class Schema extends BaseSchema {
  /**
   * Returns a query to retrieve data from the connected data source.
   *
   * @param  Object options Query options.
   * @return Object         An instance of `Query`.
   */
  query(options) {
    var defaults = {
      connection: this.connection(),
      model: this.model()
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
   * @return Boolean
   */
  create(options) {
    var defaults = {
      soft: true
    };
    options = extend({}, defaults, options);

    if (this._source === undefined) {
      throw new Error("Missing table name for this schema.");
    }

    var query = this.connection().dialect().statement('create table');
    query.ifNotExists(options.soft)
         .table(this._source)
         .columns(this.columns())
         .constraints(this.meta('constraints'))
         .meta(this.meta('table'));

    return this.connection().query(query.toString());
  }

  /**
   * Inserts and/or updates an entity and its direct relationship data in the datasource.
   *
   * @param Object   entity  The entity instance to save.
   * @param Object   options Options:
   *                         - `'validate'`  _Boolean_: If `false`, validation will be skipped, and the record will
   *                                                    be immediately saved. Defaults to `true`.
   *                         - `'whitelist'` _Object_ : An array of fields that are allowed to be saved to this record.
   *                         - `'locked'`    _Boolean_: Lock data to the schema fields.
   *                         - `'embed'`     _Object_ : List of relations to save.
   * @return Promise         Returns a promise.
   */
  save(entity, options) {
    return co(function*() {

      var defaults = {
        validate: true,
        whitelist: undefined,
        locked: this.locked(),
        embed: entity.schema().relations()
      };

      options = extend({}, defaults, options);

      options.validate = false;

      if (options.embed === true) {
        options.embed = entity.hierarchy();
      }

      options.embed = this.treeify(options.embed);

      var success = yield this._save(entity, 'belongsTo', options);

      if (!success) {
        return false;
      }

      var hasRelations = ['hasMany', 'hasOne'];

      if (entity.exists() && !entity.modified()) {
        return yield this._save(entity, hasRelations, options);
      }

      var fields = this.names();
      var whitelist = options.whitelist;

      if (whitelist || options.locked) {
        whitelist = whitelist ? whitelist : fields;
      }

      var exclude = {}, values = {}, field;
      var diff = arrayDiff(this.relations(false), fields);

      for (field of diff) {
        exclude[field] = true;
      }

      for (field of fields) {
        if (!exclude[field] && entity.get(field) !== undefined) {
          values[field] = entity.get(field);
        }
      }

      if (entity.exists() === false) {
        success = yield this.insert(values);
      } else {
        var id = entity.id();
        if (id === undefined) {
          throw new Error("Missing ID, can't update the entity.");
        }
        var params = {};
        params[this.key()] = id
        success = yield this.update(values, params);
      }
      if (entity.exists() === false) {
        var id = entity.id() === undefined ? this.lastInsertId() : undefined;
        entity.sync(id, {}, { exists: true });
      }
      var ok = yield this._save(entity, hasRelations, options);

      return success && ok;

    }.bind(this));
  }

  /**
   * Save relations helper.
   *
   * @param  Object  entity  The entity instance.
   * @param  Array   types   Type of relations to save.
   * @param  Object  options Options array.
   * @return Promise         Returns a promise.
   */
  _save(entity, types, options) {
    var defaults = { embed: {} };
    options = extend({}, defaults, options);

    types = Array.isArray(types) ? types : [types];
    return co(function*() {
      var type, value, relName, rel, success = true;

      for (var type of types) {
        for (relName in options.embed) {
          value = options.embed[relName];
          rel = this.relation(relName)
          if (!rel || rel.type() !== type) {
              continue;
          }
          var ok = yield rel.save(entity, extend({}, options, { embed: value }));
          success = success && ok;
        }
      }
      return success;
    }.bind(this));
  }

  /**
   * Inserts a records  with the given data.
   *
   * @param  Object  data       Typically an object of key/value pairs that specify the new data with which
   *                            the records will be updated. For SQL databases, this can optionally be
   *                            an SQL fragment representing the `SET` clause of an `UPDATE` query.
   * @param  Object  options    Any database-specific options to use when performing the operation.
   * @return Boolean            Returns `true` if the update operation succeeded, otherwise `false`.
   */
  insert(data, options) {
    var key = this.key();

    if (data[key] === undefined) {
      var constructor = this.connection().constructor;
      data[key] = constructor.enabled('default') ? { ':plain' : 'default' } : null;
    }

    var insert = this.connection().dialect().statement('insert');
    insert.into(this.source())
          .values(data, this.type.bind(this));

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
   * @return Boolean            Returns `true` if the update operation succeeded, otherwise `false`.
   */
  update(data, conditions, options) {
    var update = this.connection().dialect().statement('update');
    update.table(this.source())
          .where(conditions)
          .values(data, this.type.bind(this));

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
   * @param  Object   options    Any database-specific options to use when performing the operation. See
   *                             the `delete()` method of the corresponding backend database for available
   *                             options.
   * @return Boolean             Returns `true` if the remove operation succeeded, otherwise `false`.
   */
  truncate(conditions, options) {
    var del = this.connection().dialect().statement('delete');

    del.from(this.source())
         .where(conditions);

    return this.connection().query(del.toString());
  }

  /**
   * Drops the schema
   *
   * @param  array   options An array of options.
   * @return boolean
   * @throws DatabaseException If no connection is defined or the schema name is missing.
   */
  drop(options) {
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

    return this.connection().query(query.toString());
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
  _format(mode, type, value, options) {
    var formatter;
    if (value !== null && typeof value === 'object' && value.constructor === Object) {
      var key = Object.keys(value)[0];
      var connection = this.connection();
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

export default Schema;
