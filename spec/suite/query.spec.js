var co = require('co');
var extend = require('extend-merge').extend;
var merge = require('extend-merge').merge;
var Model = require('chaos-orm').Model;
var Schema = require('../../src/schema');
var Query = require('../../src/query');
var Sqlite = require('../adapter/sqlite');

var Fixtures = require('../fixture/fixtures');
var GalleryFixture = require('../fixture/schema/gallery-fixture');
var GalleryDetailFixture = require('../fixture/schema/gallery-detail-fixture');
var ImageFixture = require('../fixture/schema/image-fixture');
var ImageTagFixture = require('../fixture/schema/image-tag-fixture');
var TagFixture = require('../fixture/schema/tag-fixture');

describe("Query", function() {

  beforeEach(function(done) {
    co(function*() {
      this.connection = new Sqlite({ database: ':memory:' });
      this.fixtures = new Fixtures({
        connection: this.connection,
        fixtures: {
          gallery: GalleryFixture,
          gallery_detail: GalleryDetailFixture,
          image: ImageFixture,
          image_tag: ImageTagFixture,
          tag: TagFixture
        }
      });

      yield this.fixtures.populate('gallery', ['create']);
      yield this.fixtures.populate('gallery_detail', ['create']);
      yield this.fixtures.populate('image', ['create']);
      yield this.fixtures.populate('image_tag', ['create']);
      yield this.fixtures.populate('tag', ['create']);

      this.gallery = this.fixtures.get('gallery').model();
      this.galleryDetail = this.fixtures.get('gallery_detail').model();
      this.image = this.fixtures.get('image').model();
      this.image_tag = this.fixtures.get('image_tag').model();
      this.tag = this.fixtures.get('tag').model();

      this.query = new Query({
        model: this.gallery
      });

    }.bind(this)).then(function() {
      done();
    });
  });

  afterEach(function(done) {
    co(function*() {
      yield this.fixtures.drop();
      this.fixtures.reset();
    }.bind(this)).then(function() {
      done();
    });
  });

  describe(".construct()", function() {

    it("throws an error if no schema is available", function() {

      var closure = function() {
        this.query = new Query();
      }.bind(this);

      expect(closure).toThrow(new Error("Error, missing schema for this query."));

    });

  });

  describe(".statement()", function() {

    it("returns the select statement", function() {

      var statement = this.query.statement();
      expect(statement.constructor.name).toBe('Select');

    });

  });

  describe(".all()", function() {

    it("finds all records", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.order(['id']).all();
        expect(result.data()).toEqual([
          { id: 1, name: 'Foo Gallery' },
          { id: 2, name: 'Bar Gallery' }
        ]);
        expect(result.exists()).toBe(true);
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("filering out some fields", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.fields('name').order(['id']).all();
        expect(result.data()).toEqual([
          { name: 'Foo Gallery' },
          { name: 'Bar Gallery' }
        ]);
      }.bind(this)).then(function(result) {
        done();
      });
    });

  });

  describe(".get()", function() {

    it("finds all records", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.order(['id']).get();
        expect(result.data()).toEqual([
          { id: 1, name: 'Foo Gallery' },
          { id: 2, name: 'Bar Gallery' }
        ]);
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("finds all records using object hydration", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.order(['id']).get({ return: 'object' });

        expect(result).toEqual([
          { id: 1, name: 'Foo Gallery' },
          { id: 2, name: 'Bar Gallery' }
        ]);
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("throws an error if the return mode is not supported", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var exception;

        yield this.query.get({ return: 'unsupported' }).catch(function(err) {
          exception = err;
        });

        expect(exception).toEqual(new Error("Invalid `'unsupported'` mode as `'return'` value"));

      }.bind(this)).then(function(result) {
        done();
      });

    });

    context("using queries with no model", function() {

      beforeEach(function() {
        this.query = new Query({
          schema: this.gallery.definition()
        });
      });

      it("finds all records using object hydration", function(done) {

        co(function*() {
          yield this.fixtures.populate('gallery');

          var result = yield this.query.order(['id']).get({ return: 'object' });

          expect(result).toEqual([
            { id: 1, name: 'Foo Gallery' },
            { id: 2, name: 'Bar Gallery' }
          ]);
        }.bind(this)).then(function(result) {
          done();
        });

      });


      it("throws an error if the return mode has been set to `'entity'`", function(done) {

        co(function*() {
          yield this.fixtures.populate('gallery');

          var exception;

          yield this.query.get().catch(function(err) {
            exception = err;
          });

          expect(exception).toEqual(new Error("Missing model for this query, set `'return'` to `'object'` to get row data."));

        }.bind(this)).then(function(result) {
          done();
        });

      });

    });

  });

  describe(".first()", function() {

    it("finds the first record", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.order(['id']).first();
        expect(result.data()).toEqual({ id: 1, name: 'Foo Gallery' });

      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".fields()", function() {

    it("sets an aliased COUNT(*) field", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.fields([
          { ':as': [ { ':plain': 'COUNT(*)' }, { ':name': 'count' } ] }
        ]).first({ return: 'object' });

        expect(result).toEqual({ 'count': 2 });
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".where()", function() {

    it("filters out according conditions", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.where({ name: 'Foo Gallery' }).get();
        expect(result.count()).toBe(1);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".conditions()", function() {

    it("filters out according conditions", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.conditions({ name: 'Foo Gallery' }).get();
        expect(result.count()).toBe(1);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".group()", function() {

    it("groups by a field name", function(done) {

      co(function*() {
        yield this.fixtures.populate('image');

        var query = new Query({
          model: this.image
        });
        var result = yield query.fields(['gallery_id'])
                                .group('gallery_id')
                                .get();
        expect(result.count()).toBe(2);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".having()", function() {

    it("filters out according conditions", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var result = yield this.query.fields(['name'])
                                     .group('name')
                                     .having({ name: 'Foo Gallery' })
                                     .get();
        expect(result.count()).toBe(1);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".order()", function() {

    it("order by a field name ASC", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var query = new Query({
          model: this.gallery
        });
        var entity = yield query.order({ name: 'ASC' }).first();
        expect(entity.get('name')).toBe('Bar Gallery');

        var entity = yield this.query.order('name').first();
        expect(entity.get('name')).toBe('Bar Gallery');
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("order by a field name DESC", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var entity = yield this.query.order({ name: 'DESC' }).first();
        expect(entity.get('name')).toBe('Foo Gallery');
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".page()", function() {

    it("returns records at a specific page", function(done) {

      co(function*() {
        yield this.fixtures.populate('tag');

        var query = new Query({
          model: this.tag
        });

        var result = yield query.order(['id']).page(1).limit(3).all();
        expect(result.data()).toEqual([
          {id: 1, name: 'High Tech'},
          {id: 2, name: 'Sport'},
          {id: 3, name: 'Computer'}
        ]);

        result = yield query.order(['id']).page(2).limit(3).all();
        expect(result.data()).toEqual([
          {id: 4, name: 'Art'},
          {id: 5, name: 'Science'},
          {id: 6, name: 'City'}
        ]);
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("populates the meta count value", function(done) {

      co(function*() {
        yield this.fixtures.populate('tag');

        var query = new Query({
          model: this.tag
        });

        var result = yield query.order(['id']).page(1).limit(3).all();
        expect(result.meta()).toEqual(
          { count: 6 }
        );

      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".offset()", function() {

    it("returns records at a specific offset", function(done) {

      co(function*() {
        yield this.fixtures.populate('tag');

        var query = new Query({
          model: this.tag
        });

        var result = yield query.order(['id']).offset(0).limit(3).all();
        expect(result.data()).toEqual([
          {id: 1, name: 'High Tech'},
          {id: 2, name: 'Sport'},
          {id: 3, name: 'Computer'}
        ]);

        result = yield query.order(['id']).offset(3).limit(3).all();
        expect(result.data()).toEqual([
          {id: 4, name: 'Art'},
          {id: 5, name: 'Science'},
          {id: 6, name: 'City'}
        ]);
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("populates the meta count value", function(done) {

      co(function*() {
        yield this.fixtures.populate('tag');

        var query = new Query({
          model: this.tag
        });

        var result = yield query.order(['id']).offset(3).limit(3).all();
        expect(result.meta()).toEqual(
          { count: 6 }
        );

      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".embed()", function() {

    it("gets/sets with relationship", function() {

      var query = new Query({
        schema: new Schema({
          connection: this.connection
        })
      });
      query.embed('relation1.relation2');
      query.embed('relation3', {
        conditions: [{ title: 'hello world' }]
      });
      expect(query.embed()).toEqual([
        'relation1.relation2',
        {
          'relation3': {
            conditions: [{ title: 'hello world' }]
          }
        }
      ]);

    });

    it("loads external relations embed a custom condition on tags", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');
        yield this.fixtures.populate('image');
        yield this.fixtures.populate('image_tag');
        yield this.fixtures.populate('tag');

        var galleries = yield this.query.embed([{
          images: function(query) {
            query.where({ title: 'Las Vegas' });
          }
        }]).order('id').all();

        expect(galleries.get(0).get('images').count()).toBe(1);
        expect(galleries.get(1).get('images').count()).toBe(0);
      }.bind(this)).then(function(result) {
        done();
      });

    });

    it("loads external relations with a custom condition on tags using an array syntax", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');
        yield this.fixtures.populate('image');
        yield this.fixtures.populate('image_tag');
        yield this.fixtures.populate('tag');

        var galleries = yield this.query.embed([
          { images: { conditions: [{ title: 'Las Vegas' }] } }
        ]).order('id').all();

        expect(galleries.get(0).get('images').count()).toBe(1);
        expect(galleries.get(1).get('images').count()).toBe(0);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".has()", function() {

    it("sets a constraint on a nested relation", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');
        yield this.fixtures.populate('image');
        yield this.fixtures.populate('image_tag');
        yield this.fixtures.populate('tag');

        var galleries = yield this.query.has('images.tags', { name: 'Science' }).get();

        expect(galleries.count()).toBe(1);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".count()", function() {

    it("finds all records", function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery');

        var count = yield this.query.count();
        expect(count).toBe(2);
      }.bind(this)).then(function(result) {
        done();
      });

    });

  });

  describe(".alias()", function() {

    it("returns the alias value of table name by default", function() {

      expect(this.query.alias()).toBe('gallery');

    });

    it("gets/sets some alias values", function() {

      var schema = this.image.definition();

      expect(this.query.alias('images', schema)).toBe('image');
      expect(this.query.alias('images')).toBe('image');

    });

    it("creates unique aliases when a same table is used multiple times", function() {

      var schema = this.gallery.definition();

      expect(this.query.alias()).toBe('gallery');
      expect(this.query.alias('parent', schema)).toBe('gallery__0');
      expect(this.query.alias('parent.parent', schema)).toBe('gallery__1');
      expect(this.query.alias('parent.parent.parent', schema)).toBe('gallery__2');

    });

    it("throws an exception if a relation has no alias defined", function() {

      var closure = function() {
        this.query.alias('images');
      }.bind(this);

      expect(closure).toThrow(new Error("No alias has been defined for `'images'`."));

    });

  });

  describe(".toString()", function() {

    it("returns the SQL version of the query", function() {

      this.query.has('images.tags', { name: 'Science' });

      var expected = 'SELECT * FROM "gallery"';
      expected += ' LEFT JOIN "image" ON "gallery"."id" = "image"."gallery_id"';
      expected += ' LEFT JOIN "image_tag" ON "image"."id" = "image_tag"."image_id"';
      expected += ' LEFT JOIN "tag" ON "image_tag"."tag_id" = "tag"."id" WHERE "tag"."name" = \'Science\''
      expect(this.query.toString()).toBe(expected);
      expect(this.query.toString()).toBe(expected);

    });

  });

});
