import co from 'co';
import { extend, merge } from 'extend-merge';
import { Collector } from 'chaos-orm';

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
   *                      - `'type'`       _string_ : The type of query.
   *                      - `'connection'` _object_ : The connection instance.
   *                      - `'model'`      _string_ : The model class.
   */
  constructor(config) {
    var defaults = {
      connection: undefined,
      model: undefined,
      query: {}
    };
    config = extend({}, defaults, config);

    /**
     * The connection to the datasource.
     *
     * @var Function
     */
    this._connection = config.connection;

    /**
     * The fully namespaced model class name on which this query is starting.
     *
     * @var String
     */
    this._model = config.model;

    /**
     * Count the number of identical aliases in a query for building unique aliases.
     *
     * @var Object
     */
    this._aliasCounter = {};

    /**
     * Map beetween relation pathsand corresponding aliases.
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
     * The select statement instance.
     *
     * @var Function
     */
    this._statement = this.connection().dialect().statement('select');

    var model = this.model();

    if (model) {
      var schema = model.schema();
      var source = schema.source();
      var from = {};
      from[source] = this.alias('', schema);
      this.statement().from(from);
    }
    for (var key in config.query) {
      this[key](config.query[key]);
    }
  }

  /**
   * Gets the connection object to which this query is bound.
   *
   * @return Function Returns a connection instance.
   */
  connection() {
    if (!this._connection) {
      throw new Error("Error, missing connection for this query.");
    }
    return this._connection;
  }

  /**
   * Gets model.
   *
   * @return Function Returns the mode.
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
        collector: undefined,
        'return': 'entity'
      };
      options = extend({}, defaults, options);

      var classname = this.constructor.classes().collector;
      var collector = options.collector = options.collector ? options.collector : new classname();

      this._applyHas();

      var noFields = !this.statement().data('fields').length;
      if (noFields) {
        var star = {};
        star[this.alias()] = ['*'];
        this.statement().fields([star]);
      }

      var collection;
      var ret = options['return'];

      var cursor = yield this.connection().query(this.statement().toString());

      var model = this._model;

      switch (ret) {
        case 'entity':
          var schema = model.schema();
          var source = schema.source();
          var primaryKey = schema.primaryKey();

          collection = model.create(collection, { collector: collector, type: 'set' });

          for (var record of cursor) {
            if (record[primaryKey] && collector.exists(source, record[primaryKey])) {
              collection.push(collector.get(source, record[primaryKey]));
            } else {
              collection.push(model.create(record, {
                collector: collector,
                exists: noFields ? true : null
              }));
            }
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
      return model.schema().embed(collection, this._embed, { fetchOptions: options });
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
      return Array.isArray(result) ? result[0] : result.get(0);
    }.bind(this));
  }

  /**
   * Executes the query and returns the count number.
   *
   * @return integer The number of rows in result.
   */
  count() {
    return co(function*() {
      var schema = this.model().schema();
      this.statement().fields([{':plain': 'COUNT(*)'}]);
      var cursor = yield this.connection().query(this.statement().toString());
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

    var schema = this.model().schema();

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
    if (typeof has === "string" && arguments.length === 2) {
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

  _applyHas() {
    var schema = this.model().schema();
    var tree = schema.treeify(this.has());
    this._applyJoins(this.model(), tree, '', this.alias());
    var has = this.has();
    for (var value of has) {
      var key = Object.keys(value)[0];
      var conditions = value[key];
      this.where(conditions, this.alias(key));
    }
  }

  _applyJoins(model, tree, basePath, aliasFrom) {
    for (var key in tree) {
      var childs = tree[key];
      var rel = model.relation(key);
      var path = basePath ? basePath + '.' + key : key;
      var to;

      if (rel.type() !== 'hasManyThrough') {
        to = this._join(path, rel, aliasFrom);
      } else {
        var name = rel.using();
        var nameThrough = rel.through();
        var pathThrough = path ? path + '.' + nameThrough : nameThrough;
        var from = rel.from();

        var relThrough = from.relation(nameThrough);
        var aliasThrough = this._join(pathThrough, relThrough, aliasFrom);

        var modelThrough = relThrough.to();
        var relTo = modelThrough.relation(name);
        to = this._join(path, relTo, aliasThrough);
      }

      if (childs && Object.keys(childs).length) {
        this._applyJoins(rel.to(), childs, path, to);
      }
    }
  }

  /**
   * Set a query's join according a Relationship.
   *
   * @param  string path      The relation path.
   * @param  object rel       A Relationship instance.
   * @param  string fromAlias The "from" model alias.
   * @return string            The "to" model alias.
   */
  _join(path, rel, fromAlias) {
    if (this._aliases[path] !== undefined) {
      return this._aliases[path];
    }

    var model = rel.to();
    var schema = model.schema();
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
}

/**
 * Class dependencies.
 *
 * @var array
 */
Query._classes = {
  collector: Collector
};

export default Query;
