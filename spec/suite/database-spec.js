import { Database, Schema } from '../../src';
import { Dialect } from 'sql-dialect';
import { Source } from 'chaos-orm';

describe("Database", function() {

  beforeEach(function() {

    this.dialect = function() {}
    this.dialect.quote = function(string) {
      return "'" + string + "'";
    };

    this.database = new Database({
      dialect: this.dialect
    });
  });

  describe(".config()", function() {

    it("returns the default config", function() {

      var database = new Database();

      expect(database.config()).toEqual({
        host: 'localhost',
        username: 'root',
        password: '',
        options: {},
        meta: { key: 'id', locked: true }
      });

    });

    it("overrides the default config", function() {

      var database = new Database({
        host: 'mydomain',
        username: 'username',
        password: 'password',
        database: 'mydb',
        options: { option: 'value' },
        meta: { key: '_id', locked: false }
      });

      expect(database.config()).toEqual({
        host: 'mydomain',
        username: 'username',
        password: 'password',
        database: 'mydb',
        options: { option: 'value' },
        meta: { key: '_id', locked: false }
      });

    });

  });

  describe(".dialect()", function() {

    it("returns the dialect", function() {

      expect(this.database.dialect()).toBe(this.dialect);

    });

  });

  describe(".format()", function() {

    it("formats `null` values", function() {

      expect(this.database.format('datasource', 'id', null)).toBe('NULL');
      expect(this.database.format('datasource', 'serial', null)).toBe('NULL');
      expect(this.database.format('datasource', 'integer', null)).toBe('NULL');
      expect(this.database.format('datasource', 'float', null)).toBe('NULL');
      expect(this.database.format('datasource', 'decimal', null)).toBe('NULL');
      expect(this.database.format('datasource', 'date', null)).toBe('NULL');
      expect(this.database.format('datasource', 'datetime', null)).toBe('NULL');
      expect(this.database.format('datasource', 'boolean', null)).toBe('NULL');
      expect(this.database.format('datasource', 'null', null)).toBe('NULL');
      expect(this.database.format('datasource', 'string', null)).toBe('NULL');
      expect(this.database.format('datasource', '_default_',null)).toBe('NULL');
      expect(this.database.format('datasource', '_undefined_', null)).toBe('NULL');

    });

  });


});
