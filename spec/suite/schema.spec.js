var co = require('co');
var extend = require('extend-merge').extend;
var merge = require('extend-merge').merge;
var Database = require('../../src/database');
var Query = require('../../src/query');
var Schema = require('../../src/schema');
var Dialect = require('sql-dialect').Dialect;
var Model = require('chaos-orm').Model;
var Sqlite = require('../adapter/sqlite');

var Fixtures = require('../fixture/fixtures');
var GalleryFixture = require('../fixture/schema/gallery-fixture');
var GalleryDetailFixture = require('../fixture/schema/gallery-detail-fixture');
var ImageFixture = require('../fixture/schema/image-fixture');
var ImageTagFixture = require('../fixture/schema/image-tag-fixture');
var TagFixture = require('../fixture/schema/tag-fixture');

describe("Schema", function() {

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

  describe(".constructor()", function() {

    it("correctly sets config options", function() {

      class Connection {
        formatters() {
          return []
        }
      }

      var connection = new Connection();

      var schema = new Schema({
        connection: connection,
      });

      expect(schema.connection()).toBe(connection);

    });

  });

  describe(".connection()", function() {

    it("gets/sets the connection", function() {

      var connection = { formatters: function() { return []; }};
      var schema = new Schema();

      expect(schema.connection(connection)).toBe(schema);
      expect(schema.connection()).toBe(connection);

    });

  });

  describe(".create()/.drop()", function() {

    it("creates/drop a table", function(done) {

      co(function*() {

        yield this.fixtures.drop();

        var schema = new Schema({
          connection: this.connection,
          source: 'test_table'
        });
        schema.column('id', { type: 'serial' });

        yield schema.create();
        expect(yield this.connection.sources()).toEqual({ test_table: 'test_table' });

        yield schema.drop();
        expect(yield this.connection.sources()).toEqual({});
        done();
      }.bind(this));

    });

    it("throws an exception when source is not set on create", function(done) {

      co(function*() {
        var schema = new Schema({ connection: this.connection });
        yield schema.create();
      }.bind(this)).catch(function(e) {
        expect(e).toEqual(new Error("Missing table name for this schema."));
        done();
      });

    });

    it("throws an exception when source is not set on drop", function(done) {

      co(function*() {
        var schema = new Schema({ connection: this.connection });
        yield schema.drop();
      }.bind(this)).catch(function(e) {
        expect(e).toEqual(new Error("Missing table name for this schema."));
        done();
      });

    });

  });

  describe(".defaults()", function() {

    it("returns defaults", function() {

      var schema = new Schema({ connection: this.connection });
      schema.column('name', { type: 'string', default: 'Enter The Name Here' });
      schema.column('title', { type: 'string', default: 'Enter The Title Here', length: 50 });

      expect(schema.defaults()).toEqual({
        name: 'Enter The Name Here',
        title: 'Enter The Title Here'
      });

    });

  });

  context("with all data populated", function() {

    beforeEach(function(done) {

      co(function*() {
        yield this.fixtures.populate('gallery', ['records']);
        yield this.fixtures.populate('gallery_detail', ['records']);
        yield this.fixtures.populate('image', ['records']);
        yield this.fixtures.populate('image_tag', ['records']);
        yield this.fixtures.populate('tag', ['records']);
        done();
      }.bind(this));

    });

    describe(".embed()", function() {

      it("embeds a hasMany relationship", function(done) {

        co(function*() {
          var model = this.gallery;
          var schema = model.definition();
          var galleries = yield model.all();

          yield schema.embed(galleries, ['images']);

          for (var gallery of galleries) {
            for (var image of gallery.get('images')) {
              expect(image.get('gallery_id')).toBe(gallery.get('id'));
            }
          }

          done();
        }.bind(this));

      });

      it("embeds a belongsTo relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all();

          yield schema.embed(images, ['gallery']);

          for (var image of images) {
            expect(image.get('gallery').get('id')).toBe(image.get('gallery_id'));
          }

          done();
        }.bind(this));

      });

      it("embeds a hasOne relationship", function(done) {

        co(function*() {
          var model = this.gallery;
          var schema = model.definition();
          var galleries = yield model.all();

          yield schema.embed(galleries, ['detail', 'images']);

          for (var gallery of galleries) {
            expect(gallery.get('detail').get('gallery_id')).toBe(gallery.get('id'));
          }

          done();
        }.bind(this));

      });

      it("embeds a hasManyTrough relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all();

          yield schema.embed(images, ['tags']);

          for (var image of images) {
            image.get('images_tags').forEach(function(image_tag, index) {
              expect(image.get('tags').get(index)).toBe(image_tag.get('tag'));
            });
          }

          done();
        }.bind(this));

      });

      it("embeds nested hasManyTrough relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all();

          yield schema.embed(images, ['tags.images']);

          for (var image of images) {
            image.get('images_tags').forEach(function(image_tag, index) {
              expect(image.get('tags').get(index)).toBe(image_tag.get('tag'));
              image_tag.get('tag').get('images_tags').forEach(function(image_tag2, index2) {
                expect(image_tag.get('tag').get('images').get(index2)).toBe(image_tag2.get('image'));
              });
            });
          }
          done();
        }.bind(this));

      });

      it("embeds nested hasManyTrough relationship using object hydration", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all({}, { return: 'object' });

          yield schema.embed(images, ['tags.images'], { fetchOptions: { return: 'object' } });

          for (var image of images) {
            image.images_tags.forEach(function(image_tag, index) {
              expect(image.tags[index]).toBe(image_tag.tag);
              image_tag.tag.images_tags.forEach(function(image_tag2, index2) {
                expect(image_tag.tag.images[index2]).toBe(image_tag2.image);
              });
            });
          }
          done();
        }.bind(this));

      });

    });

    context("using the lazy strategy", function() {

      it("embeds a hasMany relationship", function(done) {

        co(function*() {
          var model = this.gallery;
          var schema = model.definition();
          var galleries = yield model.all();

          for (var gallery of galleries) {
            for (var image of yield gallery.fetch('images')) {
              expect(image.get('gallery_id')).toBe(gallery.get('id'));
            }
          }

          done();
        }.bind(this));

      });

      it("embeds a belongsTo relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all();

          for (var image of images) {
            expect((yield image.fetch('gallery')).get('id')).toBe(image.get('gallery_id'));
          }

          done();
        }.bind(this));

      });

      it("embeds a hasOne relationship", function(done) {

        co(function*() {
          var model = this.gallery;
          var schema = model.definition();
          var galleries = yield model.all();

          for (var gallery of galleries) {
            expect((yield gallery.fetch('detail')).get('gallery_id')).toBe(gallery.get('id'));
          }

          done();
        }.bind(this));

      });

      it("embeds a hasManyTrough relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all();

          for (var image of images) {
            (yield image.fetch('images_tags')).forEach(function*(image_tag, index) {
              expect((yield image.fetch('tags')).get(index)).toBe(image_tag.get('tag'));
            });
          }

          done();
        }.bind(this));

      });

      it("embeds nested hasManyTrough relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.definition();
          var images = yield model.all();

          for (var image of images) {
            (yield image.fetch('images_tags')).forEach(function*(image_tag, index) {
              expect((yield image.fetch('tags')).get(index)).toBe(image_tag.get('tag'));
              (yield image_tag.get('tag').fetch('images_tags')).forEach(function*(image_tag2, index2) {
                expect((yield image_tag.get('tag').fetch('images')).get(index2)).toBe(image_tag2.get('image'));
              });
            });
          };
          done();
        }.bind(this));

      });

    });

  });

  describe(".save()", function() {

    it("saves empty entities", function(done) {

      co(function*() {
        var Image = this.image;
        var image = Image.create();
        yield image.save();
        expect(image.exists()).toBe(true);
        done();
      }.bind(this));

    });

    it("uses whitelist with locked schema", function(done) {

      co(function*() {
        var Image = this.image;
        var image = Image.create();

        image.set({
          name: 'image',
          title: 'Image',
          gallery_id: 3
        });

        yield image.save({ whitelist: ['title'] });
        expect(image.exists()).toBe(true);

        var reloaded = yield Image.load(image.id());
        expect(reloaded.data()).toEqual({
          id: image.id(),
          name: null,
          title: 'Image',
          gallery_id: null
        });
        done();
      }.bind(this));

    });

    it("casts data on insert using datasource handlers", function(done) {

      co(function*() {
        var schema = new Schema({ source: 'test' });
        schema.connection(this.connection);

        schema.column('id',         { type: 'serial' });
        schema.column('name',       { type: 'string' });
        schema.column('null',       { type: 'string', null: true });
        schema.column('value',      { type: 'integer' });
        schema.column('double',     { type: 'float' });
        schema.column('revenue',    {
          type: 'decimal',
          length: 20,
          precision: 2
        });
        schema.column('active',     { type: 'boolean' });
        schema.column('registered', { type: 'date' });
        schema.column('created',    { type: 'datetime' });

        yield schema.create();

        yield schema.insert({
          id: 1,
          name: 'test',
          null: null,
          value: 1234,
          double: 1.5864,
          revenue: '152000.8589',
          active: true,
          registered: new Date(Date.UTC(2016, 6, 30, 0, 0, 0)),
          created: new Date(Date.UTC(2016, 6, 30, 4, 38, 55))
        });

        var cursor = yield schema.connection().query('SELECT * FROM test WHERE id = 1');
        var data = cursor.next();

        expect(data).toEqual({
          id: 1,
          name: 'test',
          null: null,
          value: 1234,
          double: 1.5864,
          revenue: 152000.86,
          active: 1,
          registered: '2016-07-30',
          created: '2016-07-30 04:38:55'
        });

        cursor.close();
        yield schema.drop();
      }.bind(this)).then(function() {
        done();
      });
    });

    it("saves and updates an entity", function(done) {

      co(function*() {
        var data = {
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        };

        var Image = this.image;
        var image = Image.create(data);
        yield image.save()
        expect(image.exists()).toBe(true);
        expect(image.id()).not.toBe(null);
        expect(image.modified()).toBe(false);

        var reloaded = yield Image.load(image.id());
        expect(reloaded.data()).toEqual({
          id: image.id(),
          gallery_id: null,
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        });

        reloaded.set('title', 'Amiga 1260');
        yield reloaded.save();
        expect(reloaded.exists()).toBe(true);
        expect(reloaded.id()).toBe(image.id());
        expect(reloaded.modified()).toBe(false);

        var persisted = yield Image.load(reloaded.id());
        expect(persisted.data()).toEqual({
          id: reloaded.id(),
          gallery_id: null,
          name: 'amiga_1200.jpg',
          title: 'Amiga 1260'
        });
      }.bind(this)).then(function() {
        done();
      });
    });

    it("saves a hasMany relationship", function(done) {

      co(function*() {
        var data = {
          name: 'Foo Gallery',
          images: [
            { name: 'amiga_1200.jpg', title: 'Amiga 1200' },
            { name: 'srinivasa_ramanujan.jpg', title: 'Srinivasa Ramanujan' },
            { name: 'las_vegas.jpg', title: 'Las Vegas' },
          ]
        };

        var Gallery = this.gallery;
        var gallery = Gallery.create(data);
        yield gallery.save({embed: 'images'});

        expect(gallery.id()).not.toBe(null);
        for (var image of gallery.get('images')) {
          expect(image.get('gallery_id')).toBe(gallery.id());
        }

        var result = yield Gallery.load(gallery.id(), { embed: ['images'] });
        expect(result.data()).toEqual(gallery.data());
      }.bind(this)).then(function() {
        done();
      });

    });

    it("saves a belongsTo relationship", function(done) {

      co(function*() {
        var data = {
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200',
          gallery: {
            name: 'Foo Gallery'
          }
        };

        var Image = this.image;
        var image = Image.create(data);
        yield image.save({ embed: 'gallery' })

        expect(image.id()).not.toBe(null);
        expect(image.get('gallery').id()).toBe(image.get('gallery_id'));

        var result = yield Image.load(image.id(), { embed: ['gallery'] });
        expect(result.data()).toEqual(image.data());
      }.bind(this)).then(function() {
        done();
      });

    });

    it("saves a hasOne relationship", function(done) {

      co(function*() {
        var data = {
          name: 'Foo Gallery',
          detail: {
            description: 'Foo Gallery Description'
          }
        };

        var Gallery = this.gallery;
        var gallery = Gallery.create(data);

        yield gallery.save({ embed: 'detail' });

        expect(gallery.id()).not.toBe(null);
        expect(gallery.get('detail').get('gallery_id')).toBe(gallery.id());

        var result = yield Gallery.load(gallery.id(), { embed: ['detail'] });
        expect(gallery.data()).toEqual(result.data());
      }.bind(this)).then(function() {
        done();
      });

    });

    context("with a hasManyTrough relationship", function() {

      beforeEach(function(done) {

        co(function*() {
          var data = {
            name: 'amiga_1200.jpg',
            title: 'Amiga 1200',
            gallery: { name: 'Foo Gallery' },
            tags: [
              { name: 'tag1' },
              { name: 'tag2' },
              { name: 'tag3' }
            ]
          };

          var Image = this.image;
          this.entity = Image.create(data);
          yield this.entity.save({ embed: ['gallery', 'tags']});
        }.bind(this)).then(function() {
          done();
        });

      });

      it("saves a hasManyTrough relationship", function(done) {

        co(function*() {
          expect(this.entity.id()).not.toBe(null);
          expect(this.entity.get('images_tags').count()).toBe(3);
          expect(this.entity.get('tags').count()).toBe(3);

          this.entity.get('images_tags').forEach(function(image_tag, index) {
            expect(image_tag.get('tag_id')).toBe(image_tag.get('tag').id());
            expect(image_tag.get('image_id')).toBe(this.entity.id());
            expect(image_tag.get('tag')).toBe(this.entity.get('tags').get(index));
          }.bind(this));

          var Image = this.image;
          var result = yield Image.load(this.entity.id(), { embed: ['gallery', 'tags'] });
          expect(this.entity.data()).toEqual(result.data());
        }.bind(this)).then(function() {
          done();
        });

      });

      it("appends a hasManyTrough entity", function(done) {

        co(function*() {
          var Image = this.image;
          var reloaded = yield Image.load(this.entity.id(), { embed: 'tags' });
          reloaded.get('tags').push({ name: 'tag4' });
          expect(reloaded.get('tags').count()).toBe(4);

          reloaded.get('tags').unset(0);
          expect(reloaded.get('tags').count()).toBe(3);

          yield reloaded.save({ embed: 'tags' });

          var persisted = yield Image.load(reloaded.id(), { embed: 'tags' });

          expect(persisted.get('tags').count()).toBe(3);

          persisted.get('images_tags').forEach(function(image_tag, index) {
            expect(image_tag.get('tag_id')).toBe(image_tag.get('tag').id());
            expect(image_tag.get('image_id')).toBe(persisted.id());
            expect(image_tag.get('tag')).toBe(persisted.get('tags').get(index));
          });
        }.bind(this)).then(function() {
          done();
        });

      });

    });

    it("saves a nested entities", function(done) {

      co(function*() {
        var data = {
          name: 'Foo Gallery',
          images: [
            {
              name: 'amiga_1200.jpg',
              title: 'Amiga 1200',
              tags: [
                { name: 'tag1' },
                { name: 'tag2' },
                { name: 'tag3' }
              ]
            }
          ]
        };

        var Gallery = this.gallery;
        var gallery = Gallery.create(data);
        yield gallery.save({ embed: 'images.tags' });

        expect(gallery.id()).not.toBe(null);
        expect(gallery.get('images').count()).toBe(1);

        for (var image of gallery.get('images')) {
          expect(image.get('gallery_id')).toBe(gallery.id());
          expect(image.get('images_tags').count()).toBe(3);
          expect(image.get('tags').count()).toBe(3);

          image.get('images_tags').forEach(function(image_tag, index) {
            expect(image_tag.get('tag_id')).toBe(image_tag.get('tag').id());
            expect(image_tag.get('image_id')).toBe(image.id());
            expect(image_tag.get('tag')).toBe(image.get('tags').get(index));
          });
        }

        var result = yield Gallery.load(gallery.id(), { embed: ['images.tags'] });
        expect(gallery.data()).toEqual(result.data());

      }.bind(this)).then(function() {
        done();
      });

    });

    it("throws an exception when trying to update an entity with no ID data", function() {

      var closure = function() {
        var Gallery = this.gallery;
        var gallery = Gallery.create({}, { exists: true });
      }.bind(this);

      expect(closure).toThrow(new Error("Existing entities must have a valid ID."));

    });

    context("with transactions", function() {

      it("commits on success", function(done) {

        co(function*() {
          var id;
          yield this.connection.transaction(function() {
            return co(function*() {
              var image = this.image.create();
              yield image.save();
              id = image.id();
            }.bind(this));
          }.bind(this));
          expect(yield this.image.load(id)).toBeAnInstanceOf(this.image);
          expect(this.connection.transactionLevel()).toBe(0);
          done();
        }.bind(this));

      });

      it("allows manual commit", function(done) {

        co(function*() {
          var image = this.image.create();
          yield this.connection.beginTransaction();
          yield image.save();

          var id = image.id();
          yield this.connection.commit();
          expect(yield this.image.load(id)).toBeAnInstanceOf(this.image);
          expect(this.connection.transactionLevel()).toBe(0);
          done();
        }.bind(this));

      });

      it("rollbacks on error", function(done) {

        co(function*() {
          var id;
          var closure = function() {
            return this.connection.transaction(function() {
              return co(function*() {
                var image = this.image.create();
                yield image.save();
                id = image.id();
                throw new Error('Error Processing.');
              }.bind(this));
            }.bind(this)).catch(function(exception) {
            });
          }.bind(this);

          try {
            yield closure();
          } catch (exception) {
            expect(exception).toEqual(new Error('Error Processing.'));
          }
          expect(yield this.image.load(id)).toBe(null);
          expect(this.connection.transactionLevel()).toBe(0);
          done();
        }.bind(this));

      });

      it("allows manual rollback", function(done) {

        co(function*() {
          var image = this.image.create();
          yield this.connection.beginTransaction();
          yield image.save();
          id = image.id();
          yield this.connection.rollback();
          expect(yield this.image.load(id)).toBe(null);
          expect(this.connection.transactionLevel()).toBe(0);
          done();
        }.bind(this));

      });

      it("supports save points", function(done) {

        co(function*() {
          var image = this.image.create({ name: 'Initial' });
          yield image.save();
          id = image.id();
          yield this.connection.beginTransaction();
          image.set('name', 'Update1');
          yield image.save();
          yield this.connection.beginTransaction();
          image.set('name', 'Update2');
          yield image.save();
          yield this.connection.beginTransaction();
          image.set('name', 'Update3');
          yield image.save();
          expect((yield this.image.load(id)).get('name')).toBe('Update3');
          yield this.connection.rollback(2);
          expect((yield this.image.load(id)).get('name')).toBe('Update2');
          yield this.connection.rollback(1);
          expect((yield this.image.load(id)).get('name')).toBe('Update1');
          yield this.connection.rollback();
          expect((yield this.image.load(id)).get('name')).toBe('Initial');
          expect(this.connection.transactionLevel()).toBe(0);
          done();
        }.bind(this));

      });

    });

  });

  describe(".truncate()", function() {

    it("deletes an entity", function(done) {

      co(function*() {
        var data = {
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        };

        var Image = this.image;
        var image = Image.create(data);

        yield image.save()
        expect(image.exists()).toBe(true);

        yield image.delete();
        expect(image.exists()).toBe(false);

      }.bind(this)).then(function() {
        done();
      });

    });

  });

  describe(".format()", function() {

    it("formats according default `'database'` handlers", function() {

      var schema = new Schema({
        connection: this.connection,
        source: 'test_table'
      });
      schema.column('id', { type: 'serial' });

      expect(schema.format('datasource', 'id', 123)).toBe('123');
      expect(schema.format('datasource', 'id', { ':plain': 'default' })).toBe('default');

    });

  });

});
