import co from 'co';
import { extend, merge } from 'extend-merge';
import { Database, Query, Schema } from '../../src';
import { Dialect } from 'sql-dialect';
import { Model } from 'chaos-orm';
import Sqlite from '../adapter/sqlite';

import Fixtures from '../fixture/fixtures';
import GalleryFixture from '../fixture/schema/gallery-fixture';
import GalleryDetailFixture from '../fixture/schema/gallery-detail-fixture';
import ImageFixture from '../fixture/schema/image-fixture';
import ImageTagFixture from '../fixture/schema/image-tag-fixture';
import TagFixture from '../fixture/schema/tag-fixture';

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


  describe(".query()", function() {

    it("throw an exception when no model is set", function() {

      var closure = function() {
        var schema = new Schema({ connection: this.connection });
        schema.query();
      }.bind(this);

      expect(closure).toThrow(new Error("Missing model for this schema, can't create a query."));

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
        schema.set('id', { type: 'serial' });

        yield schema.create();
        expect(yield this.connection.sources()).toEqual({ test_table: 'test_table' });

        yield schema.drop();
        expect(yield this.connection.sources()).toEqual({});

      }.bind(this)).then(function() {
        done();
      });

    });

    it("throw an exception when source is not set", function() {

      var closure = function() {
        var schema = new Schema({ connection: this.connection });
        schema.create();
      }.bind(this);

      expect(closure).toThrow(new Error("Missing table name for this schema."));

    });

    it("throw an exception when source is not set", function() {

      var closure = function() {
        var schema = new Schema({ connection: this.connection });
        schema.drop();
      }.bind(this);

      expect(closure).toThrow(new Error("Missing table name for this schema."));

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
      }.bind(this)).then(function() {
        done();
      });

    });

    describe(".embed()", function() {

      it("embeds a hasMany relationship", function(done) {

        co(function*() {
          var model = this.gallery;
          var schema = model.schema();
          var galleries = yield model.all();

          yield schema.embed(galleries, ['images']);

          for (var gallery of galleries) {
            for (var image of gallery.get('images')) {
              expect(image.get('gallery_id')).toBe(gallery.get('id'));
            };
          };
        }.bind(this)).then(function() {
          done();
        });

      });

      it("embeds a belongsTo relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.schema();
          var images = yield model.all();

          yield schema.embed(images, ['gallery']);

          for (var image of images) {
            expect(image.get('gallery').get('id')).toBe(image.get('gallery_id'));
          };
        }.bind(this)).then(function() {
          done();
        });

      });

      it("embeds a hasOne relationship", function(done) {

        co(function*() {
          var model = this.gallery;
          var schema = model.schema();
          var galleries = yield model.all();

          yield schema.embed(galleries, ['detail', 'images']);

          for (var gallery of galleries) {
            expect(gallery.get('detail').get('gallery_id')).toBe(gallery.get('id'));
          };
        }.bind(this)).then(function() {
          done();
        });

      });

      it("embeds a hasManyTrough relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.schema();
          var images = yield model.all();

          yield schema.embed(images, ['tags']);

          for (var image of images) {
            image.get('images_tags').forEach(function(image_tag, index) {
              expect(image.get('tags').get(index)).toBe(image_tag.get('tag'));
            });
          };
        }.bind(this)).then(function() {
          done();
        });

      });

      it("embeds nested hasManyTrough relationship", function(done) {

        co(function*() {
          var model = this.image;
          var schema = model.schema();
          var images = yield model.all();

          yield schema.embed(images, ['tags.images']);

          for (var image of images) {
            image.get('images_tags').forEach(function(image_tag, index) {
              expect(image.get('tags').get(index)).toBe(image_tag.get('tag'));
              image_tag.get('tag').get('images_tags').forEach(function(image_tag2, index2) {
                expect(image_tag.get('tag').get('images').get(index2)).toBe(image_tag2.get('image'));
              });
            });
          };
        }.bind(this)).then(function() {
          done();
        });

      });

    });

  });

  describe(".save()", function() {

    it("saves empty entities", function(done) {

      co(function*() {
        var Image = this.image;
        var image = Image.create();
        expect(yield image.save()).toBe(true);
        expect(image.exists()).toBe(true);
        done();
      }.bind(this));

    });

    it("saves and updates an entity", function(done) {

      co(function*() {
        var data = {
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        };

        var Image = this.image;
        var image = Image.create(data);
        expect(yield image.save()).toBe(true);
        expect(image.exists()).toBe(true);
        expect(image.primaryKey()).not.toBe(null);

        var reloaded = yield Image.id(image.primaryKey());
        expect(reloaded.data()).toEqual({
          id: image.primaryKey(),
          gallery_id: null,
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        });

        reloaded.set('title', 'Amiga 1260');
        expect(yield reloaded.save()).toBe(true);
        expect(reloaded.exists()).toBe(true);
        expect(reloaded.primaryKey()).toBe(image.primaryKey());

        var persisted = yield Image.id(reloaded.primaryKey());
        expect(persisted.data()).toEqual({
          id: reloaded.primaryKey(),
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
        expect(yield gallery.save()).toBe(true);

        expect(gallery.primaryKey()).not.toBe(null);
        for (var image of gallery.get('images')) {
          expect(image.get('gallery_id')).toBe(gallery.primaryKey());
        }

        var result = yield Gallery.id(gallery.primaryKey(), { embed: ['images'] });
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
        expect(yield image.save()).toBe(true);

        expect(image.primaryKey()).not.toBe(null);
        expect(image.get('gallery').primaryKey()).toBe(image.get('gallery_id'));

        var result = yield Image.id(image.primaryKey(), { embed: ['gallery'] });
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

        expect(yield gallery.save()).toBe(true);

        expect(gallery.primaryKey()).not.toBe(null);
        expect(gallery.get('detail').get('gallery_id')).toBe(gallery.primaryKey());

        var result = yield Gallery.id(gallery.primaryKey(), { embed: ['detail'] });
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
          yield this.entity.save();
        }.bind(this)).then(function() {
          done();
        });

      });

      it("saves a hasManyTrough relationship", function(done) {

        co(function*() {
          expect(this.entity.primaryKey()).not.toBe(null);
          expect(this.entity.get('images_tags').count()).toBe(3);
          expect(this.entity.get('tags').count()).toBe(3);

          this.entity.get('images_tags').forEach(function(image_tag, index) {
            expect(image_tag.get('tag_id')).toBe(image_tag.get('tag').primaryKey());
            expect(image_tag.get('image_id')).toBe(this.entity.primaryKey());
            expect(image_tag.get('tag')).toBe(this.entity.get('tags').get(index));
          }.bind(this));

          var Image = this.image;
          var result = yield Image.id(this.entity.primaryKey(), { embed: ['gallery', 'tags'] });
          expect(this.entity.data()).toEqual(result.data());
        }.bind(this)).then(function() {
          done();
        });

      });

      it("appends a hasManyTrough entity", function(done) {

        co(function*() {
          var Image = this.image;
          var reloaded = yield Image.id(this.entity.primaryKey(), { embed: ['tags'] });
          reloaded.get('tags').push({ name: 'tag4' });
          expect(reloaded.get('tags').count()).toBe(4);

          reloaded.get('tags').unset(0);
          expect(reloaded.get('tags').count()).toBe(3);

          expect(yield reloaded.save()).toBe(true);

          var persisted = yield Image.id(reloaded.primaryKey(), { embed: ['tags'] });

          expect(persisted.get('tags').count()).toBe(3);

          persisted.get('images_tags').forEach(function(image_tag, index) {
            expect(image_tag.get('tag_id')).toBe(image_tag.get('tag').primaryKey());
            expect(image_tag.get('image_id')).toBe(persisted.primaryKey());
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
        expect(yield gallery.save({ embed: 'images.tags' })).toBe(true);

        expect(gallery.primaryKey()).not.toBe(null);
        expect(gallery.get('images').count()).toBe(1);

        for (var image of gallery.get('images')) {
          expect(image.get('gallery_id')).toBe(gallery.primaryKey());
          expect(image.get('images_tags').count()).toBe(3);
          expect(image.get('tags').count()).toBe(3);

          image.get('images_tags').forEach(function(image_tag, index) {
            expect(image_tag.get('tag_id')).toBe(image_tag.get('tag').primaryKey());
            expect(image_tag.get('image_id')).toBe(image.primaryKey());
            expect(image_tag.get('tag')).toBe(image.get('tags').get(index));
          });
        }

        var result = yield Gallery.id(gallery.primaryKey(), { embed: ['images.tags'] });
        expect(gallery.data()).toEqual(result.data());

      }.bind(this)).then(function() {
        done();
      });

    });

    it("validates by default", function(done) {

      co(function*() {
        var Image = this.image;
        var image = Image.create();
        Image.validator().rule('name', 'not:empty');

        expect(yield image.save()).toBe(false);
        expect(image.exists()).toBe(false);
        done();
      }.bind(this));

    });

    it("validates direct relationships by default", function(done) {

      co(function*() {
        var Gallery = this.gallery;
        Gallery.validator().rule('name', 'not:empty');

        var Image = this.image;
        var image = Image.create({
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200',
          gallery: {}
        });
        expect(yield image.save()).toBe(false);
        expect(image.exists()).toBe(false);
        done();
      }.bind(this));

    });

    it("throws an exception when trying to update an entity with no ID data", function(done) {

      var Gallery = this.gallery;
      var gallery = Gallery.create({}, { exists: true });
      gallery.set('name', 'Foo Gallery');
      gallery.save()
        .then(function() {
          expect(false).toBe(true);
          done();
        })
        .catch(function(err) {
          expect(err).toEqual(new Error("Missing ID, can't update the entity."));
          done();
        });

    });

  });

  describe(".persist()", function() {

    it("saves an entity", function(done) {

      co(function*() {
        var data = {
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        };

        var Image = this.image;
        var image = Image.create(data);

        var spy = spyOn(image, 'save').and.callThrough();

        expect(yield image.persist({ custom: 'option' })).toBe(true);
        expect(image.exists()).toBe(true);
        expect(image.primaryKey()).not.toBe(null);

        expect(spy).toHaveBeenCalledWith({
          custom: 'option',
          embed: false
        });

      }.bind(this)).then(function() {
        done();
      });

    });

  });

  describe(".delete()", function() {

    it("deletes an entity", function(done) {

      co(function*() {
        var data = {
          name: 'amiga_1200.jpg',
          title: 'Amiga 1200'
        };

        var Image = this.image;
        var image = Image.create(data);

        expect(yield image.save()).toBe(true);
        expect(image.exists()).toBe(true);

        yield image.delete();
        expect(image.exists()).toBe(false);

      }.bind(this)).then(function() {
        done();
      });

    });

  });

});
