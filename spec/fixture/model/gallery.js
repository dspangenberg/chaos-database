import { Model } from 'chaos-orm';
import Image from './image';
import GalleryDetail from './gallery-detail';

class Gallery extends Model {

  static _define(schema) {
    schema.column('id', { type: 'serial' });
    schema.column('name', { type: 'string' });

    schema.hasOne('detail', 'GalleryDetail', { keys: { id: 'gallery_id' } });
    schema.hasMany('images', 'Image', { keys: { id: 'gallery_id' } });
  }
}

Gallery.register();

export default Gallery;