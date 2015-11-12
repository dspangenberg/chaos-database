import { extend, merge } from 'extend-merge';
import { Schema as BaseSchema, Relationship, BelongsTo, HasOne, HasMany, HasManyThrough } from 'chaos-orm';
import Query from './query';

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
         .columns(this.fields())
         .constraints(this.meta('constraints'))
         .meta(this.meta('table'));

    return this.connection().query(query.toString());
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
    var insert = this.connection().dialect().statement('insert');
    insert.into(this.source())
          .values(data);

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
   * @param  Object   options    Any database-specific options to use when performing the operation. See
   *                             the `delete()` method of the corresponding backend database for available
   *                             options.
   * @return Boolean             Returns `true` if the remove operation succeeded, otherwise `false`.
   */
  delete(conditions, options) {
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
    var sequence = this.source() + '_' + this.primaryKey() + '_seq';
    return this.connection().lastInsertId(sequence);
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
