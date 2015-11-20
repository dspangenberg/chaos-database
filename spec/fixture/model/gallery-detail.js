import { Model } from 'chaos-orm';
import Gallery from './gallery';

class GalleryDetail extends Model {

  static _define(schema) {
    schema.set('id', { type: 'serial' });
    schema.set('description', { type: 'string' });
    schema.set('gallery_id', { type: 'integer' });

    schema.belongsTo('gallery',  'Gallery', { keys: { gallery_id: 'id' } });
  }
}

GalleryDetail.register();

export default GalleryDetail;
