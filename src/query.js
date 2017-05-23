var co = require('co');
var extend = require('extend-merge').extend;
var merge = require('extend-merge').merge;
var Model = require('chaos-orm').Model;

/**
 * The Query wrapper.
 */
class Query {

  /**
   * Gets/sets classes dependencies.
   *
   * @param  Object classes The classes dependencies to set or none to get it.
   * @return mixed          The classes dependencies.
   */
  static classes(classes) {
    if (arguments.length) {
      this._classes = extend({}, this._classes, classes);
    }
    return this._classes;
  }

  /**
   * Creates a new record object with default values.
   *
   * @param array config Possible options are:
   *                      - `'model'`  _Function_ : A model class.
   *                      - `'schema'` _Object_   : Alternatively a schema instance can be provided instead of the model.
   *                      - `'query'`  _Object_   : The query.
   */
  constructor(config) {
    var defaults = {
      model: undefined,
      schema: undefined,
      query: {}
    };
    config = extend({}, defaults, config);

    if (config.model) {
      this._model = config.model;
      this._schema = this._model.definition();
    } else {
      this._schema = config.schema;
    }

    /**
     * Count the number of identical aliases in a query for building unique aliases.
     *
     * @var Object
     */
    this._aliasCounter = {};

    /**
     * Map beetween relation paths and corresponding aliases.
     *
     * @var Object
     */
    this._aliases = {};

    /**
     * Map beetween generated aliases and corresponding schema.
     *
     * @var Object
     */
    this._schemas = {};

    /**
     * The relations to include.
     *
     * @var Array
     */
    this._embed = [];

    /**
     * Some conditions over some relations.
     *
     * @var Array
     */
    this._has = [];

    /**
     * Pagination.
     *
     * @var Array
     */
    this._page = [];

    var schema = this.schema();

    /**
     * The select statement instance.
     *
     * @var Function
     */
    this._statement = schema.connection().dialect().statement('select');

    var source = schema.source();
    var from = {};
    from[source] = this.alias('', schema);
    this.statement().from(from);

    for (var key in config.query) {
      if (typeof this[key] !== 'function') {
        throw new Error("Invalid option `'" + key + "'` as query options.");
      }
      this[key](config.query[key]);
    }
  }

  /**
   * Gets the schema.
   *
   * @return Function Returns the schema.
   */
  schema() {
    if (!this._schema) {
      throw new Error("Error, missing schema for this query.");
    }
    return this._schema;
  }

  /**
   * Gets the model.
   *
   * @return Function Returns the model.
   */
  model() {
    return this._model;
  }

  /**
   * Gets the query statement.
   *
   * @return Function Returns a connection instance.
   */
  statement() {
    return this._statement;
  }

  /**
   * Executes the query and returns the result.
   *
   * @param  Object  Options The fetching options.
   * @return Promise         A Promise.
   */
  get(options) {
    return co(function*(){
      var defaults = {
        return: 'entity'
      };
      options = extend({}, defaults, options);

      this._applyHas();
      this._applyLimit();

      var schema = this.schema();
      var statement = this.statement();

      var allFields = !statement.data('fields').length;
      if (allFields) {
        var star = {};
        star[this.alias()] = ['*'];
        statement.fields([star]);
      }

      if (statement.data('joins').length) {
        this.group(schema.key());
      }

      var collection;
      var ret = options['return'];

      var cursor = yield schema.connection().query(statement.toString(this._schemas, this._aliases));

      var key = schema.key();

      switch (ret) {
        case 'entity':
          var model = this.model();
          if (!model) {
            throw new Error("Missing model for this query, set `'return'` to `'object'` to get row data.");
          }

          collection = model.create([], {
            type: 'set',
            exists: true
          });

          if (this.statement().data('limit')) {
            var count = this.count();
            collection.meta({ count: yield count });
          }

          for (var record of cursor) {
            collection.push(model.create(record, {
              exists: record[key] !== undefined ? true : null
            }));
          }
          break;
        case 'array':
        case 'object':
          collection = [];
          for (var record of cursor) {
            collection.push(extend({}, record));
          }
          break;
        default:
          throw new Error("Invalid `'" + options['return'] + "'` mode as `'return'` value");
          break;
      }
      return schema.embed(collection, this._embed, { fetchOptions: options });
    }.bind(this));
  }

  /**
   * Alias for `get()`
   *
   * @return object An iterator instance.
   */
  all(options) {
    return this.get(options);
  }

  /**
   * Executes the query and returns the first result only.
   *
   * @return object An entity instance.
   */
  first(options) {
    return co(function*() {
      var result = yield this.get(options);
      return result.length ? (Array.isArray(result) ? result[0] : result.get(0)) : null;
    }.bind(this));
  }

  /**
   * Executes the query and returns the count number.
   *
   * @return integer The number of rows in result.
   */
  count() {
    return co(function*() {
      this._applyHas();
      var statement = this.statement();
      var counter = this.schema().connection().dialect().statement('select');

      var primaryKey = statement.dialect().name(this.alias() + '.' + this.schema().key());
      counter.fields({ ':plain': 'COUNT(DISTINCT ' + primaryKey + ') as count' });
      counter.data('from', statement.data('from'));
      counter.data('joins', statement.data('joins'));
      counter.data('where', statement.data('where'));
      counter.data('group', statement.data('group'));
      counter.data('having', statement.data('having'));
      var cursor = yield this.schema().connection().query('SELECT SUM(count) FROM(' + counter.toString(this._schemas, this._aliases) + ') x');
      var result = cursor.next();
      var key = Object.keys(result)[0]
      return Number.parseInt(result[key]);
    }.bind(this));
  }

  /**
   * Adds some fields to the query
   *
   * @param  mixed    fields The fields.
   * @return Function        Returns `this`.
   */
  fields(fields) {
    fields = Array.isArray(fields) && arguments.length === 1 ? fields : Array.prototype.slice.call(arguments);

    var schema = this.schema();

    for (var value of fields) {
      if (typeof value === 'string' && schema.has(value)) {
        var aliased = {};
        aliased[this.alias()] = [value]
        this.statement().fields(aliased);
      } else {
        this.statement().fields(value);
      }
    }
    return this;
  }

  /**
   * Adds some where conditions to the query
   *
   * @param  mixed    conditions The conditions for this query.
   * @return Function            Returns `this`.
   */
  where(conditions, alias) {
    var conditions = this.statement().dialect().prefix(conditions, alias ? alias : this.alias(), false);
    this.statement().where(conditions);
    return this;
  }

  /**
   * Alias for `where()`.
   *
   * @param  mixed    conditions The conditions for this query.
   * @return Function            Returns `this`.
   */
  conditions(conditions) {
    return this.where(conditions);
  }

  /**
   * Adds some group by fields to the query
   *
   * @param  mixed    fields The fields.
   * @return Function        Returns `this`.
   */
  group(fields) {
    fields = Array.isArray(fields) && arguments.length === 1 ? fields : Array.prototype.slice.call(arguments);
    fields = this.statement().dialect().prefix(fields, this.alias());
    this.statement().group(fields);
    return this;
  }

  /**
   * Adds some having conditions to the query
   *
   * @param  mixed    conditions The conditions for this query.
   * @return Function            Returns `this`.
   */
  having(conditions) {
    conditions = this.statement().dialect().prefix(conditions, this.alias());
    this.statement().having(conditions);
    return this;
  }

  /**
   * Adds some order by fields to the query
   *
   * @param  mixed    fields The fields.
   * @return Function        Returns `this`.
   */
  order(fields) {
    fields = Array.isArray(fields) && arguments.length === 1 ? fields : Array.prototype.slice.call(arguments);
    fields = this.statement().dialect().prefix(fields, this.alias());
    this.statement().order(fields);
    return this;
  }

  /**
   * Sets page number.
   *
   * @param  integer page The page number
   * @return self
   */
  page(page)
  {
    this._page.page = page;
    return this;
  }

  /**
   * Sets offset value.
   *
   * @param  integer offset The offset value.
   * @return self
   */
  offset(offset)
  {
    this._page.offset = offset;
    return this;
  }

  /**
   * Sets limit value.
   *
   * @param  integer limit The number of results to limit or `0` for limit at all.
   * @return self
   */
  limit(limit)
  {
    this._page.limit = Number.parseInt(limit);
    return this;
  }

  /**
   * Applies a query handler
   *
   * @param  Closure  closure A closure.
   * @return Function         Returns `this`.
   */
  handler(closure) {
    if (typeof closure === 'function') {
      closure(this);
    }
    return this;
  }

  /**
   * Sets the relations to retrieve.
   *
   * @param  array  embed The relations to load with the query.
   * @return object        Returns `this`.
   */
  embed(embed, conditions) {
    if (!arguments.length) {
      return this._embed;
    }
    if (typeof embed === "string" && arguments.length === 2) {
      var mix = {};
      mix[embed] = conditions || [];
      embed = [mix];
    } else {
      embed = Array.isArray(embed) ? embed : [embed];
    }
    this._embed = this._embed.concat(embed);
    return this;
  }

  /**
   * Sets the conditionnal dependency over some relations.
   *
   * @param array The conditionnal dependency.
   */
  has(has, conditions) {
    if (!arguments.length) {
      return this._has;
    }
    if (typeof has === 'string') {
      var mix = {};
      mix[has] = conditions || [];
      has = [mix];
    }
    this._has = this._has.concat(has)
    return this;
  }

  /**
   * Gets a unique alias for the query or a query's relation if `relpath` is set.
   *
   * @param  string path   A dotted relation name or for identifying the query's relation.
   * @param  object schema The corresponding schema to alias.
   * @return string        A string alias.
   */
  alias(path, schema) {
    if (arguments.length < 2) {
      path = path || '';
      if (this._aliases[path] !== undefined) {
        return this._aliases[path];
      } else {
        throw new Error("No alias has been defined for `'" + path + "'`.");
      }
    }

    var alias = schema.source();
    if (this._aliasCounter[alias] === undefined) {
      this._aliasCounter[alias] = 0;
      this._aliases[path] = alias;
    } else {
      alias = this._aliases[path] = alias + '__' + this._aliasCounter[alias]++;
    }
    this._schemas[alias] = schema;
    return alias;
  }

  /**
   * Applies the has conditions.
   */
  _applyHas() {
    var schema = this.schema();
    var tree = schema.treeify(this.has());
    this._applyJoins(this.schema(), tree, '', this.alias());
    var has = this.has();
    for (var value of has) {
      var key = Object.keys(value)[0];
      var conditions = value[key];
      this.where(conditions, this.alias(key));
    }
    this._has = [];
  }

  /**
   * Applies the limit range when applicable.
   */
  _applyLimit() {
    if (!this._page.limit) {
        return;
    }
    var offset;
    if (this._page.offset) {
      offset = this._page.offset;
    } else {
      var page = this._page.page ? this._page.page : 1;
      offset = (page - 1) * this._page.limit;
    }
    this.statement().limit(this._page.limit, offset);
  }

  /**
   * Applies joins.
   *
   * @param Object schema    The schema to perform joins on.
   * @param Array  tree      The tree of relations to join.
   * @param Array  basePath  The base relation path.
   * @param String aliasFrom The alias name of the from model.
   */
  _applyJoins(schema, tree, basePath, aliasFrom) {
    for (var key in tree) {
      var children = tree[key] && tree[key].embed ? tree[key].embed : {};
      var rel = schema.relation(key);
      var path = basePath ? basePath + '.' + key : key;
      var to;

      if (rel.type() !== 'hasManyThrough') {
        to = this._join(path, rel, aliasFrom);
      } else {
        var name = rel.using();
        var nameThrough = rel.through();
        var pathThrough = path ? path + '.' + nameThrough : nameThrough;
        var from = rel.from();

        var relThrough = from.definition().relation(nameThrough);
        var aliasThrough = this._join(pathThrough, relThrough, aliasFrom);

        var modelThrough = relThrough.to();
        var relTo = modelThrough.definition().relation(name);
        to = this._join(path, relTo, aliasThrough);
      }

      if (children && Object.keys(children).length) {
        this._applyJoins(rel.to().definition(), children, path, to);
      }
    }
  }

  /**
   * Set a query's join according a Relationship.
   *
   * @param  string path      The relation path.
   * @param  object rel       A Relationship instance.
   * @param  string fromAlias The "from" model alias.
   * @return string           The "to" model alias.
   */
  _join(path, rel, fromAlias) {
    if (this._aliases[path] !== undefined) {
      return this._aliases[path];
    }

    var schema = rel.to().definition();
    var source = schema.source();
    var toAlias = this.alias(path, schema);

    var table = {};
    table[source] = toAlias;

    this.statement().join(
      table,
      this._on(rel, fromAlias, toAlias),
      'LEFT'
    );
    return toAlias;
  }

  /**
   * Build the `ON` constraints from a `Relationship` instance.
   *
   * @param  object rel       A Relationship instance.
   * @param  string fromAlias The "from" model alias.
   * @param  string toAlias   The "to" model alias.
   * @return array             A constraints array.
   */
  _on(rel, fromAlias, toAlias) {
    if (rel.type() === 'hasManyThrough') {
      return [];
    }
    var keys = rel.keys();
    var fromField = Object.keys(keys)[0];
    var toField = keys[fromField];
    return { '=': [{ ':name': fromAlias + '.' + fromField },  { ':name': toAlias + '.' + toField }] };
  }

  /**
   * Return the SQL string.
   *
   * @return String
   */
  toString() {
    var statement = this._statement;
    this._applyHas();
    this._applyLimit();

    var parts = {
      fields: statement.data('fields').slice(),
      joins: statement.data('joins').slice(),
      where: statement.data('where').slice(),
      limit: statement.data('limit')
    };

    var noFields = !statement.data('fields');
    if (noFields) {
      statement.fields({ [this.alias()]: ['*'] });
    }
    var sql = statement.toString(this._schemas, this._aliases);

    for (var name in parts) {
      statement.data(name, parts[name]);
    }
    this._aliasCounter = {};
    this._aliases = {};
    this.alias('', this.schema());
    return sql;
  }
}

module.exports = Query;
